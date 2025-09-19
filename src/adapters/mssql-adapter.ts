import mssql from 'mssql';
import type { config as MssqlConfig } from 'mssql';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, FieldInfo } from '../types/database.js';
import { ConnectionError, TransactionError, QueryError } from '../types/errors.js';

export class MSSQLAdapter extends BaseDatabaseAdapter {
  private pool: mssql.ConnectionPool | null = null;
  private transaction: mssql.Transaction | null = null;

  async connect(): Promise<void> {
    try {
      const config = this.config;
      const mssqlConfigObj: MssqlConfig = {
        server: config.host || 'localhost',
        port: config.port || 1433,
        database: config.database || 'master',
        user: config.username,
        password: config.password,
        options: {
          encrypt: config.ssl || false,
          trustServerCertificate: config.trustServerCertificate || false,
          instanceName: config.instanceName,
        },
        connectionTimeout: 30000,
        requestTimeout: 30000,
      };

      const { ConnectionPool } = mssql;
      this.pool = new ConnectionPool(mssqlConfigObj);
      await this.pool.connect();
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw new ConnectionError(
        `Failed to connect to MSSQL database: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql'
      );
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.transaction) {
        await this.transaction.rollback();
        this.transaction = null;
      }
      if (this.pool) {
        await this.pool.close();
        this.pool = null;
      }
      this.connected = false;
    } catch (error) {
      throw new ConnectionError(
        `Failed to disconnect from MSSQL database: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql'
      );
    }
  }

  async executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    if (!this.pool) {
      throw new ConnectionError('Not connected to database', 'mssql');
    }

    try {
      const request = this.transaction ? this.transaction.request() : this.pool.request();
      
      if (parameters) {
        parameters.forEach((param, index) => {
          request.input(`param${index}`, param);
        });
      }

      const result = await request.query(query);
      
      return {
        rows: result.recordset || [],
        rowCount: result.rowsAffected[0] || 0,
        fields: this.mapFields(result.recordset?.columns || {}),
      };
    } catch (error) {
      throw new QueryError(
        `Query execution failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql',
        query,
        parameters
      );
    }
  }

  async getTables(): Promise<TableInfo[]> {
    const query = `
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_NAME
    `;
    
    const result = await this.executeQuery(query);
    const tableNames = result.rows.map(row => row.TABLE_NAME as string);
    
    const tables: TableInfo[] = [];
    for (const tableName of tableNames) {
      try {
        const tableInfo = await this.getTableInfo(tableName);
        if (tableInfo) {
          tables.push(tableInfo);
        }
      } catch (error) {
        tables.push({
          name: tableName,
          schema: 'dbo',
          type: 'table',
          columns: [],
          indexes: [],
          constraints: [],
        });
      }
    }
    
    return tables;
  }

  async getTableInfo(tableName: string, schema?: string): Promise<TableInfo | null> {
    const schemaName = schema || 'dbo';
    
    const columnsQuery = `
      SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.IS_NULLABLE,
        c.COLUMN_DEFAULT,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY,
        CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_FOREIGN_KEY
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN (
        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
          ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
        AND c.TABLE_NAME = pk.TABLE_NAME 
        AND c.COLUMN_NAME = pk.COLUMN_NAME
      LEFT JOIN (
        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
          ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
      ) fk ON c.TABLE_SCHEMA = fk.TABLE_SCHEMA 
        AND c.TABLE_NAME = fk.TABLE_NAME 
        AND c.COLUMN_NAME = fk.COLUMN_NAME
      WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @tableName
      ORDER BY c.ORDINAL_POSITION
    `;

    const columnsResult = await this.executeQuery(columnsQuery, [schemaName, tableName]);
    
    const indexesQuery = `
      SELECT 
        i.name as INDEX_NAME,
        c.name as COLUMN_NAME,
        i.is_unique as IS_UNIQUE,
        i.type_desc as INDEX_TYPE
      FROM sys.indexes i
      INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
      INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
      INNER JOIN sys.tables t ON i.object_id = t.object_id
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @schema AND t.name = @tableName
      ORDER BY i.name, ic.key_ordinal
    `;

    const indexesResult = await this.executeQuery(indexesQuery, [schemaName, tableName]);
    
    const constraintsQuery = `
      SELECT 
        tc.CONSTRAINT_NAME,
        tc.CONSTRAINT_TYPE,
        ccu.COLUMN_NAME,
        ccu2.TABLE_NAME as REFERENCED_TABLE,
        ccu2.COLUMN_NAME as REFERENCED_COLUMN
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
        ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
      LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
        ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
      LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu2 
        ON rc.UNIQUE_CONSTRAINT_NAME = ccu2.CONSTRAINT_NAME
      WHERE tc.TABLE_SCHEMA = @schema AND tc.TABLE_NAME = @tableName
    `;

    const constraintsResult = await this.executeQuery(constraintsQuery, [schemaName, tableName]);

    return {
      name: tableName,
      schema: schemaName,
      type: 'table',
      columns: columnsResult.rows.map(row => ({
        name: row.COLUMN_NAME as string,
        dataType: row.DATA_TYPE as string,
        nullable: row.IS_NULLABLE === 'YES',
        defaultValue: row.COLUMN_DEFAULT,
        isPrimaryKey: Boolean(row.IS_PRIMARY_KEY),
        isForeignKey: Boolean(row.IS_FOREIGN_KEY),
        maxLength: row.CHARACTER_MAXIMUM_LENGTH as number | undefined,
        precision: row.NUMERIC_PRECISION as number | undefined,
        scale: row.NUMERIC_SCALE as number | undefined,
      })),
      indexes: this.groupIndexes(indexesResult.rows),
      constraints: this.groupConstraints(constraintsResult.rows),
    };
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    const statsQuery = `
      SELECT 
        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE') as TOTAL_TABLES,
        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS) as TOTAL_VIEWS,
        (SELECT COUNT(*) FROM sys.indexes WHERE type > 0) as TOTAL_INDEXES,
        (SELECT SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) * 8.0 / 1024) FROM sys.database_files) as DATABASE_SIZE_MB,
        (SELECT COUNT(*) FROM sys.dm_exec_connections WHERE database_id = DB_ID()) as CONNECTION_COUNT
    `;

    const result = await this.executeQuery(statsQuery);
    const stats = result.rows[0];

    if (!stats) {
      return {
        totalTables: 0,
        totalViews: 0,
        totalIndexes: 0,
        databaseSize: '0 MB',
        connectionCount: 0,
      };
    }

    return {
      totalTables: stats.TOTAL_TABLES as number,
      totalViews: stats.TOTAL_VIEWS as number,
      totalIndexes: stats.TOTAL_INDEXES as number,
      databaseSize: `${(stats.DATABASE_SIZE_MB as number).toFixed(2)} MB`,
      connectionCount: stats.CONNECTION_COUNT as number,
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

  isConnected(): boolean {
    return this.connected && this.pool !== null;
  }

  protected formatQuery(query: string, parameters?: unknown[]): string {
    if (!parameters || parameters.length === 0) {
      return query;
    }
    
    let formattedQuery = query;
    parameters.forEach((param, index) => {
      const placeholder = `@param${index}`;
      // Replace all occurrences of '?' with the parameter placeholder
      formattedQuery = formattedQuery.replace(/\?/g, placeholder);
    });
    
    return formattedQuery;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`MSSQL error: ${String(error)}`);
  }

  async beginTransaction(): Promise<void> {
    if (!this.pool) {
      throw new ConnectionError('Not connected to database', 'mssql');
    }
    
    if (this.transaction) {
      throw new TransactionError('Transaction already in progress', 'mssql');
    }

    try {
      this.transaction = new mssql.Transaction(this.pool);
      await this.transaction.begin();
    } catch (error) {
      this.transaction = null;
      throw new TransactionError(
        `Failed to begin transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql'
      );
    }
  }

  async commitTransaction(): Promise<void> {
    if (!this.transaction) {
      throw new TransactionError('No transaction in progress', 'mssql');
    }

    try {
      await this.transaction.commit();
      this.transaction = null;
    } catch (error) {
      this.transaction = null;
      throw new TransactionError(
        `Failed to commit transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql'
      );
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.transaction) {
      throw new TransactionError('No transaction in progress', 'mssql');
    }

    try {
      await this.transaction.rollback();
      this.transaction = null;
    } catch (error) {
      this.transaction = null;
      throw new TransactionError(
        `Failed to rollback transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'mssql'
      );
    }
  }

  isInTransaction(): boolean {
    return this.transaction !== null;
  }

  private mapFields(columns: Record<string, any>): FieldInfo[] {
    return Object.values(columns).map((column: any) => ({
      name: column.name,
      dataType: column.type.name || 'unknown',
      nullable: column.nullable || false,
      defaultValue: column.defaultValue,
    }));
  }

  private groupIndexes(rows: Record<string, unknown>[]): any[] {
    const indexMap = new Map();
    
    rows.forEach(row => {
      const indexName = row.INDEX_NAME as string;
      if (!indexMap.has(indexName)) {
        indexMap.set(indexName, {
          name: indexName,
          columns: [],
          unique: Boolean(row.IS_UNIQUE),
          type: row.INDEX_TYPE as string,
        });
      }
      indexMap.get(indexName).columns.push(row.COLUMN_NAME as string);
    });

    return Array.from(indexMap.values());
  }

  private groupConstraints(rows: Record<string, unknown>[]): any[] {
    const constraintMap = new Map();
    
    rows.forEach(row => {
      const constraintName = row.CONSTRAINT_NAME as string;
      if (!constraintMap.has(constraintName)) {
        constraintMap.set(constraintName, {
          name: constraintName,
          type: row.CONSTRAINT_TYPE as string,
          columns: [],
          referencedTable: row.REFERENCED_TABLE as string | undefined,
          referencedColumns: [],
        });
      }
      
      const constraint = constraintMap.get(constraintName);
      if (row.COLUMN_NAME) {
        constraint.columns.push(row.COLUMN_NAME as string);
      }
      if (row.REFERENCED_COLUMN) {
        constraint.referencedColumns.push(row.REFERENCED_COLUMN as string);
      }
    });

    return Array.from(constraintMap.values());
  }
}
