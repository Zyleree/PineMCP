import mysql, { Connection, Pool, PoolConnection } from 'mysql2/promise';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, ColumnInfo, IndexInfo, ConstraintInfo } from '../types/database.js';
import { ConnectionError, TransactionError, QueryError } from '../types/errors.js';

export class MySQLAdapter extends BaseDatabaseAdapter {
  private pool: Pool | null = null;
  private connection: Connection | null = null;
  private transactionConnection: PoolConnection | null = null;

  async connect(): Promise<void> {
    try {
      const connectionConfig: any = {
        host: this.config.host || 'localhost',
        port: this.config.port || 3306,
        database: this.config.database || 'mysql',
        user: this.config.username || 'root',
        password: this.config.password || '',
        ssl: this.config.ssl ? { rejectUnauthorized: false } : false,
        connectionLimit: 20,
        acquireTimeout: 30000,
        timeout: 2000,
      };

      this.pool = mysql.createPool(connectionConfig);
      this.connection = await this.pool.getConnection();
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw this.handleError(error);
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.transactionConnection) {
        this.transactionConnection.release();
        this.transactionConnection = null;
      }
      if (this.connection) {
        this.connection.destroy();
        this.connection = null;
      }
      if (this.pool) {
        await this.pool.end();
        this.pool = null;
      }
      this.connected = false;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  isConnected(): boolean {
    return this.connected && this.pool !== null;
  }

  async executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    const connection = this.transactionConnection || this.connection;
    if (!connection) {
      throw new ConnectionError('Database not connected', 'mysql');
    }

    try {
      const [rows, fields] = await connection.execute(query, parameters);
      
      return {
        rows: Array.isArray(rows) ? rows as Record<string, unknown>[] : [],
        rowCount: Array.isArray(rows) ? rows.length : 0,
        fields: fields ? fields.map(field => ({
          name: field.name,
          dataType: field.type?.toString() || 'UNKNOWN',
          nullable: !(field.flags && (field.flags as number & 1)), // NOT_NULL flag
          defaultValue: undefined, // MySQL2 doesn't provide defaultValue in FieldPacket
        })) : [],
      };
    } catch (error) {
      throw new QueryError(
        `MySQL query failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mysql',
        query,
        parameters
      );
    }
  }

  async getTables(): Promise<TableInfo[]> {
    const query = `
      SELECT 
        table_name,
        table_schema,
        CASE 
          WHEN table_type = 'BASE TABLE' THEN 'table'
          WHEN table_type = 'VIEW' THEN 'view'
          ELSE 'table'
        END as table_type
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
      ORDER BY table_schema, table_name
    `;

    const result = await this.executeQuery(query);
    const tables: TableInfo[] = [];

    for (const row of result.rows) {
      const tableInfo = await this.getTableInfo(row.table_name as string, row.table_schema as string);
      if (tableInfo) {
        tables.push(tableInfo);
      }
    }

    return tables;
  }

  async getTableInfo(tableName: string, schema?: string): Promise<TableInfo | null> {
    const schemaName = schema || 'information_schema';
    
    // Get table info
    const tableQuery = `
      SELECT 
        table_name,
        table_schema,
        CASE 
          WHEN table_type = 'BASE TABLE' THEN 'table'
          WHEN table_type = 'VIEW' THEN 'view'
          ELSE 'table'
        END as table_type
      FROM information_schema.tables
      WHERE table_name = ? AND table_schema = ?
    `;

    const tableResult = await this.executeQuery(tableQuery, [tableName, schemaName]);
    if (tableResult.rows.length === 0) {
      return null;
    }

    const table = tableResult.rows[0];

    // Get columns
    const columnsQuery = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        CASE WHEN column_key = 'PRI' THEN true ELSE false END as is_primary_key,
        CASE WHEN column_key = 'MUL' AND referenced_table_name IS NOT NULL THEN true ELSE false END as is_foreign_key
      FROM information_schema.columns
      WHERE table_name = ? AND table_schema = ?
      ORDER BY ordinal_position
    `;

    const columnsResult = await this.executeQuery(columnsQuery, [tableName, schemaName]);
    const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
      name: row.column_name as string,
      dataType: row.data_type as string,
      nullable: row.is_nullable === 'YES',
      defaultValue: row.column_default,
      isPrimaryKey: row.is_primary_key as boolean,
      isForeignKey: row.is_foreign_key as boolean,
      maxLength: row.character_maximum_length as number,
      precision: row.numeric_precision as number,
      scale: row.numeric_scale as number,
    }));

    // Get indexes
    const indexesQuery = `
      SELECT 
        index_name as name,
        GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
        CASE WHEN non_unique = 0 THEN true ELSE false END as unique,
        index_type as type
      FROM information_schema.statistics
      WHERE table_name = ? AND table_schema = ?
      GROUP BY index_name, non_unique, index_type
    `;

    const indexesResult = await this.executeQuery(indexesQuery, [tableName, schemaName]);
    const indexes: IndexInfo[] = indexesResult.rows.map(row => ({
      name: row.name as string,
      columns: (row.columns as string).split(','),
      unique: row.unique as boolean,
      type: row.type as string,
    }));

    // Get constraints
    const constraintsQuery = `
      SELECT 
        constraint_name as name,
        constraint_type as type,
        GROUP_CONCAT(column_name ORDER BY ordinal_position) as columns,
        referenced_table_name as referenced_table,
        GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position) as referenced_columns
      FROM information_schema.key_column_usage
      WHERE table_name = ? AND table_schema = ?
      GROUP BY constraint_name, constraint_type, referenced_table_name
    `;

    const constraintsResult = await this.executeQuery(constraintsQuery, [tableName, schemaName]);
    const constraints: ConstraintInfo[] = constraintsResult.rows.map(row => ({
      name: row.name as string,
      type: (row.type as string) as 'PRIMARY KEY' | 'FOREIGN KEY' | 'UNIQUE' | 'CHECK' | 'NOT NULL',
      columns: (row.columns as string).split(','),
      referencedTable: row.referenced_table as string,
      referencedColumns: row.referenced_columns ? (row.referenced_columns as string).split(',') : undefined,
    }));

    return {
      name: (table?.table_name as string) || '',
      schema: (table?.table_schema as string) || '',
      type: (table?.table_type as 'table' | 'view' | 'materialized_view') || 'table',
      columns,
      indexes,
      constraints,
    };
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    const tablesQuery = `
      SELECT COUNT(*) as total_tables
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `;

    const viewsQuery = `
      SELECT COUNT(*) as total_views
      FROM information_schema.views
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `;

    const indexesQuery = `
      SELECT COUNT(DISTINCT index_name) as total_indexes
      FROM information_schema.statistics
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `;

    const sizeQuery = `
      SELECT 
        ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as database_size_mb
      FROM information_schema.tables
      WHERE table_schema = DATABASE()
    `;

    const connectionsQuery = `
      SELECT COUNT(*) as connection_count
      FROM information_schema.processlist
      WHERE db = DATABASE()
    `;

    const [tablesResult, viewsResult, indexesResult, sizeResult, connectionsResult] = await Promise.all([
      this.executeQuery(tablesQuery),
      this.executeQuery(viewsQuery),
      this.executeQuery(indexesQuery),
      this.executeQuery(sizeQuery),
      this.executeQuery(connectionsQuery),
    ]);

    const sizeInMB = (sizeResult.rows[0]?.database_size_mb as number) || 0;
    const sizeFormatted = sizeInMB ? `${sizeInMB} MB` : '0 MB';

    return {
      totalTables: parseInt((tablesResult.rows[0]?.total_tables as string) || '0'),
      totalViews: parseInt((viewsResult.rows[0]?.total_views as string) || '0'),
      totalIndexes: parseInt((indexesResult.rows[0]?.total_indexes as string) || '0'),
      databaseSize: sizeFormatted,
      connectionCount: parseInt((connectionsResult.rows[0]?.connection_count as string) || '0'),
    };
  }

  async validateConnection(): Promise<boolean> {
    try {
      await this.executeQuery('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }

  async beginTransaction(): Promise<void> {
    if (this.transactionConnection) {
      throw new TransactionError('Transaction already in progress', 'mysql');
    }
    
    if (!this.pool) {
      throw new ConnectionError('Database not connected', 'mysql');
    }
    
    try {
      this.transactionConnection = await this.pool.getConnection();
      await this.transactionConnection.execute('START TRANSACTION');
    } catch (error) {
      if (this.transactionConnection) {
        this.transactionConnection.release();
        this.transactionConnection = null;
      }
      throw new TransactionError(
        `Failed to begin MySQL transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mysql'
      );
    }
  }

  async commitTransaction(): Promise<void> {
    if (!this.transactionConnection) {
      throw new TransactionError('No transaction in progress', 'mysql');
    }
    
    try {
      await this.transactionConnection.execute('COMMIT');
    } catch (error) {
      throw new TransactionError(
        `Failed to commit MySQL transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mysql'
      );
    } finally {
      this.transactionConnection.release();
      this.transactionConnection = null;
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.transactionConnection) {
      throw new TransactionError('No transaction in progress', 'mysql');
    }
    
    try {
      await this.transactionConnection.execute('ROLLBACK');
    } catch (error) {
      throw new TransactionError(
        `Failed to rollback MySQL transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mysql'
      );
    } finally {
      this.transactionConnection.release();
      this.transactionConnection = null;
    }
  }

  isInTransaction(): boolean {
    return this.transactionConnection !== null;
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
    return new Error(`MySQL error: ${String(error)}`);
  }
}
