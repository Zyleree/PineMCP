import { MongoClient, Db } from 'mongodb';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, ColumnInfo, IndexInfo } from '../types/database.js';
import { ConnectionError, TransactionError, QueryError } from '../types/errors.js';

export class MongoDBAdapter extends BaseDatabaseAdapter {
  private client: MongoClient | null = null;
  private db: Db | null = null;
  private inTransaction: boolean = false;
  private session: any = null;

  async connect(): Promise<void> {
    try {
      const connectionString = this.config.url || 
        `mongodb://${this.config.username ? `${this.config.username}:${this.config.password}@` : ''}${this.config.host || 'localhost'}:${this.config.port || 27017}/${this.config.database || 'test'}`;

      const options: any = {};
      if (this.config.authSource) {
        options.authSource = this.config.authSource;
      }
      this.client = new MongoClient(connectionString, options);

      await this.client.connect();
      this.db = this.client.db(this.config.database || 'test');
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw new ConnectionError(
        `Failed to connect to MongoDB: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb'
      );
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.session) {
        await this.session.endSession();
        this.session = null;
      }
      if (this.client) {
        await this.client.close();
        this.client = null;
        this.db = null;
      }
      this.connected = false;
    } catch (error) {
      throw new ConnectionError(
        `Failed to disconnect from MongoDB: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb'
      );
    }
  }

  isConnected(): boolean {
    return this.connected && this.client !== null && this.db !== null;
  }

  async executeQuery(query: string, _parameters?: unknown[]): Promise<QueryResult> {
    if (!this.db) {
      throw new ConnectionError('Database not connected', 'mongodb');
    }

    try {
      if (!query || typeof query !== 'string') {
        throw new QueryError('Query must be a non-empty string', 'mongodb', query);
      }

      let queryObj: any;
      try {
        queryObj = JSON.parse(query);
      } catch (parseError) {
        throw new QueryError('Invalid JSON format in query', 'mongodb', query);
      }

      if (!queryObj || typeof queryObj !== 'object') {
        throw new QueryError('Query must be a valid JSON object', 'mongodb', query);
      }

      const { collection, operation, filter, update, options } = queryObj;

      if (!collection || typeof collection !== 'string') {
        throw new QueryError('Query must include a valid collection name', 'mongodb', query);
      }

      if (!operation || typeof operation !== 'string') {
        throw new QueryError('Query must include a valid operation', 'mongodb', query);
      }

      const sanitizedCollection = this.sanitizeCollectionName(collection);
      if (!sanitizedCollection) {
        throw new QueryError('Invalid collection name', 'mongodb', query);
      }

      const coll = this.db.collection(sanitizedCollection);
      let result: unknown;
      
      const sessionOptions = this.inTransaction && this.session ? { session: this.session } : {};

      const sanitizedFilter = this.sanitizeObject(filter);
      const sanitizedUpdate = this.sanitizeObject(update);
      const sanitizedOptions = this.sanitizeObject(options);

      switch (operation.toLowerCase()) {
        case 'find':
          result = await coll.find(sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions }).toArray();
          break;
        case 'findone':
          result = await coll.findOne(sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'insertone':
          result = await coll.insertOne(sanitizedUpdate || {}, sessionOptions);
          break;
        case 'insertmany':
          result = await coll.insertMany(Array.isArray(sanitizedUpdate) ? sanitizedUpdate : [sanitizedUpdate], sessionOptions);
          break;
        case 'updateone':
          result = await coll.updateOne(sanitizedFilter || {}, sanitizedUpdate || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'updatemany':
          result = await coll.updateMany(sanitizedFilter || {}, sanitizedUpdate || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'deleteone':
          result = await coll.deleteOne(sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'deletemany':
          result = await coll.deleteMany(sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'count':
          result = await coll.countDocuments(sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'distinct':
          if (typeof sanitizedUpdate !== 'string') {
            throw new QueryError('Distinct operation requires a string field name', 'mongodb', query);
          }
          result = await coll.distinct(sanitizedUpdate, sanitizedFilter || {}, { ...sanitizedOptions, ...sessionOptions });
          break;
        case 'aggregate':
          if (!Array.isArray(sanitizedUpdate)) {
            throw new QueryError('Aggregate operation requires an array of pipeline stages', 'mongodb', query);
          }
          result = await coll.aggregate(sanitizedUpdate, { ...sanitizedOptions, ...sessionOptions }).toArray();
          break;
        default:
          throw new QueryError(`Unsupported MongoDB operation: ${operation}`, 'mongodb', query);
      }

      const rows = Array.isArray(result) ? result : [result];
      
      return {
        rows: rows.map((row, index) => ({ _id: index, ...row })),
        rowCount: rows.length,
        fields: rows.length > 0 ? Object.keys(rows[0] as Record<string, unknown>).map(key => ({
          name: key,
          dataType: 'object',
          nullable: true,
          defaultValue: undefined,
        })) : [],
      };
    } catch (error) {
      throw new QueryError(
        `MongoDB operation failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb',
        query
      );
    }
  }

  async getTables(): Promise<TableInfo[]> {
    if (!this.db) {
      throw new ConnectionError('Database not connected', 'mongodb');
    }

    try {
      const collections = await this.db.listCollections().toArray();
      const tables: TableInfo[] = [];

      for (const collection of collections) {
        const coll = this.db.collection(collection.name);
        
        // Get sample document to determine schema
        const sample = await coll.findOne({});
        const columns: ColumnInfo[] = [];

        if (sample) {
          Object.keys(sample).forEach(key => {
            columns.push({
              name: key,
              dataType: this.getMongoDBType(sample[key]),
              nullable: true,
              defaultValue: undefined,
              isPrimaryKey: key === '_id',
              isForeignKey: false,
            });
          });
        }

        // Get indexes
        const indexes = await coll.indexes();
        const indexInfos: IndexInfo[] = indexes.map(index => ({
          name: index.name || 'unnamed',
          columns: Object.keys(index.key),
          unique: index.unique || false,
          type: 'btree',
        }));

        tables.push({
          name: collection.name,
          type: 'table',
          columns,
          indexes: indexInfos,
          constraints: [],
        });
      }

      return tables;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getTableInfo(tableName: string): Promise<TableInfo | null> {
    if (!this.db) {
      throw new ConnectionError('Database not connected', 'mongodb');
    }

    try {
      const coll = this.db.collection(tableName);
      
      // Get sample document
      const sample = await coll.findOne({});
      if (!sample) {
        return null;
      }

      const columns: ColumnInfo[] = Object.keys(sample).map(key => ({
        name: key,
        dataType: this.getMongoDBType(sample[key]),
        nullable: true,
        defaultValue: undefined,
        isPrimaryKey: key === '_id',
        isForeignKey: false,
      }));

      // Get indexes
      const indexes = await coll.indexes();
      const indexInfos: IndexInfo[] = indexes.map(index => ({
        name: index.name || 'unnamed',
        columns: Object.keys(index.key),
        unique: index.unique || false,
        type: 'btree',
      }));

      return {
        name: tableName,
        type: 'table',
        columns,
        indexes: indexInfos,
        constraints: [],
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    if (!this.db) {
      throw new ConnectionError('Database not connected', 'mongodb');
    }

    try {
      const stats = await this.db.stats();
      const collections = await this.db.listCollections().toArray();
      
      return {
        totalTables: collections.length,
        totalViews: 0,
        totalIndexes: stats.indexes || 0,
        databaseSize: `${Math.round((stats.dataSize || 0) / 1024 / 1024 * 100) / 100} MB`,
        connectionCount: 1,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async validateConnection(): Promise<boolean> {
    try {
      await this.db?.admin().ping();
      return true;
    } catch {
      return false;
    }
  }

  async beginTransaction(): Promise<void> {
    if (this.inTransaction) {
      throw new TransactionError('Transaction already in progress', 'mongodb');
    }
    
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'mongodb');
    }
    
    try {
      this.session = this.client.startSession();
      this.session.startTransaction();
      this.inTransaction = true;
    } catch (error) {
      this.session = null;
      this.inTransaction = false;
      throw new TransactionError(
        `Failed to begin MongoDB transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb'
      );
    }
  }

  async commitTransaction(): Promise<void> {
    if (!this.inTransaction || !this.session) {
      throw new TransactionError('No transaction in progress', 'mongodb');
    }
    
    try {
      await this.session.commitTransaction();
      await this.session.endSession();
      this.session = null;
      this.inTransaction = false;
    } catch (error) {
      this.session = null;
      this.inTransaction = false;
      throw new TransactionError(
        `Failed to commit MongoDB transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb'
      );
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction || !this.session) {
      throw new TransactionError('No transaction in progress', 'mongodb');
    }
    
    try {
      await this.session.abortTransaction();
      await this.session.endSession();
      this.session = null;
      this.inTransaction = false;
    } catch (error) {
      this.session = null;
      this.inTransaction = false;
      throw new TransactionError(
        `Failed to rollback MongoDB transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mongodb'
      );
    }
  }

  isInTransaction(): boolean {
    return this.inTransaction;
  }

  private getMongoDBType(value: unknown): string {
    if (value === null) return 'null';
    if (Array.isArray(value)) return 'array';
    if (value instanceof Date) return 'date';
    if (typeof value === 'object') return 'object';
    if (typeof value === 'string') return 'string';
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    return 'unknown';
  }

  protected formatQuery(query: string, _parameters?: unknown[]): string {
    return query;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`MongoDB error: ${String(error)}`);
  }

  /**
   * Sanitize collection name to prevent injection attacks
   */
  private sanitizeCollectionName(collection: string): string | null {
    const sanitized = collection.replace(/[^a-zA-Z0-9_-]/g, '');
    
    if (!sanitized || sanitized.length === 0 || sanitized.length > 120) {
      return null;
    }
    
    const reservedNames = ['system', 'admin', 'local', 'config'];
    if (reservedNames.includes(sanitized.toLowerCase())) {
      return null;
    }
    
    return sanitized;
  }

  /**
   * Sanitize MongoDB query objects to prevent NoSQL injection
   * Note: This is a basic sanitization. For production use, consider using
   * MongoDB's built-in parameterized queries or a proper ODM/ORM.
   */
  private sanitizeObject(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    
    if (typeof obj === 'string') {
      return obj.replace(/\$\$where|\$where|\$eval/gi, (match) => {
        if (match.toLowerCase() === '$where' || match.toLowerCase() === '$$where' || match.toLowerCase() === '$eval') {
          throw new QueryError(`Dangerous MongoDB operator '${match}' is not allowed`, 'mongodb');
        }
        return match;
      });
    }
    
    if (Array.isArray(obj)) {
      return obj.map(item => this.sanitizeObject(item));
    }
    
    if (typeof obj === 'object') {
      const sanitized: any = {};
      for (const [key, value] of Object.entries(obj)) {
        if (key.startsWith('$') && ['$where', '$$where', '$eval'].includes(key.toLowerCase())) {
          throw new QueryError(`Dangerous MongoDB operator '${key}' is not allowed`, 'mongodb');
        }
        
        sanitized[key] = this.sanitizeObject(value);
      }
      return sanitized;
    }
    
    return obj;
  }
}
