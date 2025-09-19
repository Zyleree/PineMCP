import { Client, types } from 'cassandra-driver';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, ColumnInfo, IndexInfo } from '../types/database.js';
import { ConnectionError } from '../types/errors.js';

export class CassandraAdapter extends BaseDatabaseAdapter {
  private client: Client | null = null;
  private inTransaction: boolean = false;

  async connect(): Promise<void> {
    try {
      const connectionConfig: any = {
        contactPoints: [`${this.config.host || 'localhost'}:${this.config.port || 9042}`],
        localDataCenter: this.config.datacenter || 'datacenter1',
        keyspace: this.config.keyspace || this.config.database || 'test',
        credentials: this.config.username ? {
          username: this.config.username,
          password: this.config.password || '',
        } : undefined,
      };

      this.client = new Client(connectionConfig);
      await this.client.connect();
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw this.handleError(error);
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.client) {
        await this.client.shutdown();
        this.client = null;
      }
      this.connected = false;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  isConnected(): boolean {
    return this.connected && this.client !== null;
  }

  async executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'cassandra');
    }

    try {
      const result = await this.client.execute(query, parameters || []);
      
      const rows = result.rows.map((row, index) => {
        const obj: Record<string, unknown> = { _row_id: index };
        for (let columnIndex = 0; columnIndex < result.columns.length; columnIndex++) {
          const column = result.columns[columnIndex];
          if (column) {
            const value = row.get(column.name);
            const columnName = column.name || `column_${columnIndex}`;
            obj[columnName] = this.convertCassandraValue(value);
          }
        }
        return obj;
      });

      const fields = result.columns.map(column => ({
        name: column.name,
        dataType: this.getCassandraType(column.type),
        nullable: true,
        defaultValue: undefined,
      }));

      return {
        rows,
        rowCount: rows.length,
        fields,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getTables(): Promise<TableInfo[]> {
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'cassandra');
    }

    try {
      const query = `
        SELECT table_name, keyspace_name
        FROM system_schema.tables
        WHERE keyspace_name = ?
      `;
      
      const result = await this.client.execute(query, [this.config.keyspace || this.config.database]);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const tableName = row.get('table_name');
        const tableInfo = await this.getTableInfo(tableName);
        if (tableInfo) {
          tables.push(tableInfo);
        }
      }

      return tables;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getTableInfo(tableName: string): Promise<TableInfo | null> {
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'cassandra');
    }

    try {
      // Get table metadata
      const tableQuery = `
        SELECT table_name, keyspace_name
        FROM system_schema.tables
        WHERE keyspace_name = ? AND table_name = ?
      `;
      
      const tableResult = await this.client.execute(tableQuery, [this.config.keyspace || this.config.database, tableName]);
      if (tableResult.rows.length === 0) {
        return null;
      }

      // Get columns
      const columnsQuery = `
        SELECT column_name, type, kind
        FROM system_schema.columns
        WHERE keyspace_name = ? AND table_name = ?
        ORDER BY position
      `;
      
      const columnsResult = await this.client.execute(columnsQuery, [this.config.keyspace || this.config.database, tableName]);
      const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
        name: row.get('column_name'),
        dataType: this.getCassandraType(row.get('type')),
        nullable: row.get('kind') !== 'partition_key',
        defaultValue: undefined,
        isPrimaryKey: row.get('kind') === 'partition_key' || row.get('kind') === 'clustering',
        isForeignKey: false,
      }));

      // Get indexes
      const indexesQuery = `
        SELECT index_name, kind
        FROM system_schema.indexes
        WHERE keyspace_name = ? AND table_name = ?
      `;
      
      const indexesResult = await this.client.execute(indexesQuery, [this.config.keyspace || this.config.database, tableName]);
      const indexes: IndexInfo[] = indexesResult.rows.map(row => ({
        name: row.get('index_name'),
        columns: [], // Cassandra indexes are on single columns
        unique: false,
        type: row.get('kind'),
      }));

      return {
        name: tableName,
        type: 'table',
        columns,
        indexes,
        constraints: [],
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'cassandra');
    }

    try {
      const tablesQuery = `
        SELECT COUNT(*) as table_count
        FROM system_schema.tables
        WHERE keyspace_name = ?
      `;
      
      const tablesResult = await this.client.execute(tablesQuery, [this.config.keyspace || this.config.database]);
      const tableCount = tablesResult.rows[0]?.get('table_count') || 0;

      return {
        totalTables: tableCount,
        totalViews: 0,
        totalIndexes: 0, // Would need separate query
        databaseSize: 'Unknown', // Would need JMX metrics
        connectionCount: 1,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async validateConnection(): Promise<boolean> {
    try {
      await this.client?.execute('SELECT now() FROM system.local');
      return true;
    } catch {
      return false;
    }
  }

  async beginTransaction(): Promise<void> {
    // Cassandra doesn't support traditional transactions
    // This is a no-op for compatibility
    this.inTransaction = true;
  }

  async commitTransaction(): Promise<void> {
    // Cassandra doesn't support traditional transactions
    // This is a no-op for compatibility
    this.inTransaction = false;
  }

  async rollbackTransaction(): Promise<void> {
    // Cassandra doesn't support traditional transactions
    // This is a no-op for compatibility
    this.inTransaction = false;
  }

  isInTransaction(): boolean {
    return this.inTransaction;
  }

  private convertCassandraValue(value: unknown): unknown {
    if (value === null) return null;
    if (value instanceof types.Uuid) return value.toString();
    if (value instanceof types.TimeUuid) return value.toString();
    if (value instanceof types.InetAddress) return value.toString();
    if (value instanceof types.LocalDate) return value.toString();
    if (value instanceof types.LocalTime) return value.toString();
    if (value instanceof types.LocalDate) return value.toString();
    if (value instanceof types.Duration) return value.toString();
    if (value instanceof types.BigDecimal) return value.toString();
    if (value instanceof types.Integer) return value.toString();
    return value;
  }

  private getCassandraType(type: any): string {
    if (!type) return 'unknown';
    
    const typeName = type.constructor.name;
    switch (typeName) {
      case 'TextType':
        return 'text';
      case 'AsciiType':
        return 'ascii';
      case 'VarcharType':
        return 'varchar';
      case 'IntType':
        return 'int';
      case 'BigintType':
        return 'bigint';
      case 'SmallintType':
        return 'smallint';
      case 'TinyintType':
        return 'tinyint';
      case 'FloatType':
        return 'float';
      case 'DoubleType':
        return 'double';
      case 'DecimalType':
        return 'decimal';
      case 'BooleanType':
        return 'boolean';
      case 'DateType':
        return 'date';
      case 'TimeType':
        return 'time';
      case 'TimestampType':
        return 'timestamp';
      case 'UuidType':
        return 'uuid';
      case 'TimeuuidType':
        return 'timeuuid';
      case 'InetType':
        return 'inet';
      case 'DurationType':
        return 'duration';
      case 'ListType':
        return 'list';
      case 'SetType':
        return 'set';
      case 'MapType':
        return 'map';
      case 'TupleType':
        return 'tuple';
      case 'UserDefinedType':
        return 'udt';
      default:
        return 'unknown';
    }
  }

  protected formatQuery(query: string, parameters?: unknown[]): string {
    if (!parameters || parameters.length === 0) {
      return query;
    }
    
    let formattedQuery = query;
    parameters.forEach((param, _index) => {
      const placeholder = '?';
      const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : String(param);
      formattedQuery = formattedQuery.replace(placeholder, value);
    });
    
    return formattedQuery;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`Cassandra error: ${String(error)}`);
  }
}
