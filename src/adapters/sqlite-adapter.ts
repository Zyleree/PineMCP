import sqlite3 from 'sqlite3';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, ColumnInfo, IndexInfo, ConstraintInfo } from '../types/database.js';
import { ConnectionError, TransactionError, QueryError } from '../types/errors.js';

export class SQLiteAdapter extends BaseDatabaseAdapter {
  private db: sqlite3.Database | null = null;
  private inTransaction: boolean = false;

  async connect(): Promise<void> {
    try {
      const filename = this.config.filename || this.config.database || ':memory:';
      
      await new Promise<void>((resolve, reject) => {
        this.db = new sqlite3.Database(filename, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
      
      // Set pragmas
      await this.executeQuery('PRAGMA journal_mode = WAL');
      await this.executeQuery('PRAGMA foreign_keys = ON');
      
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw this.handleError(error);
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.db) {
        await new Promise<void>((resolve, reject) => {
          this.db!.close((err) => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        });
        this.db = null;
      }
      this.connected = false;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  isConnected(): boolean {
    return this.connected && this.db !== null;
  }

  async executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    if (!this.db) {
      throw new ConnectionError('Database not connected', 'sqlite');
    }

    return new Promise<QueryResult>((resolve, reject) => {
      this.db!.all(query, parameters || [], (err, rows) => {
        if (err) {
          reject(this.handleError(err));
          return;
        }

        // Get column information by running a similar query with LIMIT 0
        const columnQuery = query.includes('LIMIT') ? query : `${query} LIMIT 0`;
        this.db!.all(columnQuery, parameters || [], (colErr, _colRows) => {
          if (colErr) {
            // If we can't get column info, just return the data
            resolve({
              rows: rows as Record<string, unknown>[],
              rowCount: rows ? rows.length : 0,
              fields: [],
            });
            return;
          }

          // Get column names from the first row or from the query result
          const fields = rows && rows.length > 0 
            ? Object.keys(rows[0] as Record<string, unknown>).map(name => ({
                name,
                dataType: 'TEXT',
                nullable: true,
                defaultValue: undefined,
              }))
            : [];

          resolve({
            rows: rows as Record<string, unknown>[],
            rowCount: rows ? rows.length : 0,
            fields,
          });
        });
      });
    });
  }

  async getTables(): Promise<TableInfo[]> {
    const query = `
      SELECT 
        name,
        type,
        sql
      FROM sqlite_master
      WHERE type IN ('table', 'view')
      ORDER BY name
    `;

    const result = await this.executeQuery(query);
    const tables: TableInfo[] = [];

    for (const row of result.rows) {
      const tableInfo = await this.getTableInfo(row.name as string);
      if (tableInfo) {
        tables.push(tableInfo);
      }
    }

    return tables;
  }

  async getTableInfo(tableName: string): Promise<TableInfo | null> {
    // Get table info
    const tableQuery = `
      SELECT 
        name,
        type,
        sql
      FROM sqlite_master
      WHERE name = ? AND type IN ('table', 'view')
    `;

    const tableResult = await this.executeQuery(tableQuery, [tableName]);
    if (tableResult.rows.length === 0) {
      return null;
    }

    const table = tableResult.rows[0];

    // Get columns using PRAGMA table_info
    const columnsQuery = `PRAGMA table_info(${this.escapeIdentifier(tableName)})`;
    const columnsResult = await this.executeQuery(columnsQuery);
    
    const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
      name: row.name as string,
      dataType: row.type as string,
      nullable: !(row.notnull as boolean),
      defaultValue: row.dflt_value,
      isPrimaryKey: (row.pk as boolean),
      isForeignKey: false, // Will be determined separately
      maxLength: undefined,
      precision: undefined,
      scale: undefined,
    }));

    // Get foreign key information
    const fkQuery = `PRAGMA foreign_key_list(${this.escapeIdentifier(tableName)})`;
    const fkResult = await this.executeQuery(fkQuery);
    
    // Mark foreign key columns
    fkResult.rows.forEach(fk => {
      const column = columns.find(col => col.name === fk.from as string);
      if (column) {
        column.isForeignKey = true;
      }
    });

    // Get indexes
    const indexesQuery = `PRAGMA index_list(${this.escapeIdentifier(tableName)})`;
    const indexesResult = await this.executeQuery(indexesQuery);
    
    const indexes: IndexInfo[] = [];
    for (const indexRow of indexesResult.rows) {
      const indexName = indexRow.name as string;
      const unique = (indexRow.unique as boolean);
      
      // Get index columns
      const indexColumnsQuery = `PRAGMA index_info(${this.escapeIdentifier(indexName)})`;
      const indexColumnsResult = await this.executeQuery(indexColumnsQuery);
      
      const indexColumns = indexColumnsResult.rows.map(col => col.name as string);
      
      indexes.push({
        name: indexName,
        columns: indexColumns,
        unique,
        type: 'btree', // SQLite default
      });
    }

    // Get constraints (simplified for SQLite)
    const constraints: ConstraintInfo[] = [];
    
    // Primary key constraints
    const pkColumns = columns.filter(col => col.isPrimaryKey).map(col => col.name);
    if (pkColumns.length > 0) {
      constraints.push({
        name: `pk_${tableName}`,
        type: 'PRIMARY KEY',
        columns: pkColumns,
      });
    }

    // Foreign key constraints
    fkResult.rows.forEach(fk => {
      constraints.push({
        name: `fk_${tableName}_${fk.from}`,
        type: 'FOREIGN KEY',
        columns: [fk.from as string],
        referencedTable: fk.table as string,
        referencedColumns: [fk.to as string],
      });
    });

    return {
      name: (table?.name as string) || '',
      type: table?.type === 'table' ? 'table' : 'view',
      columns,
      indexes,
      constraints,
    };
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    const tablesQuery = `
      SELECT COUNT(*) as total_tables
      FROM sqlite_master
      WHERE type = 'table'
    `;

    const viewsQuery = `
      SELECT COUNT(*) as total_views
      FROM sqlite_master
      WHERE type = 'view'
    `;

    const indexesQuery = `
      SELECT COUNT(*) as total_indexes
      FROM sqlite_master
      WHERE type = 'index'
    `;

    const sizeQuery = `
      SELECT page_count * page_size as database_size_bytes
      FROM pragma_page_count(), pragma_page_size()
    `;

    const [tablesResult, viewsResult, indexesResult, sizeResult] = await Promise.all([
      this.executeQuery(tablesQuery),
      this.executeQuery(viewsQuery),
      this.executeQuery(indexesQuery),
      this.executeQuery(sizeQuery),
    ]);

    const sizeInBytes = (sizeResult.rows[0]?.database_size_bytes as number) || 0;
    const sizeInMB = Math.round(sizeInBytes / 1024 / 1024 * 100) / 100;
    const sizeFormatted = `${sizeInMB} MB`;

    return {
      totalTables: parseInt((tablesResult.rows[0]?.total_tables as string) || '0'),
      totalViews: parseInt((viewsResult.rows[0]?.total_views as string) || '0'),
      totalIndexes: parseInt((indexesResult.rows[0]?.total_indexes as string) || '0'),
      databaseSize: sizeFormatted,
      connectionCount: 1, // SQLite is single-connection
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
    if (this.inTransaction) {
      throw new TransactionError('Transaction already in progress', 'sqlite');
    }
    
    await this.executeQuery('BEGIN TRANSACTION');
    this.inTransaction = true;
  }

  async commitTransaction(): Promise<void> {
    if (!this.inTransaction) {
      throw new TransactionError('No transaction in progress', 'sqlite');
    }
    
    await this.executeQuery('COMMIT');
    this.inTransaction = false;
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction) {
      throw new TransactionError('No transaction in progress', 'sqlite');
    }
    
    await this.executeQuery('ROLLBACK');
    this.inTransaction = false;
  }

  isInTransaction(): boolean {
    return this.inTransaction;
  }

  private escapeIdentifier(identifier: string): string {
    // Simple identifier escaping for SQLite
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  protected formatQuery(query: string, parameters?: unknown[]): string {
    if (!parameters || parameters.length === 0) {
      return query;
    }
    
    let formattedQuery = query;
    let paramIndex = 0;
    
    // Replace all ? placeholders with actual parameter values
    for (let i = 0; i < formattedQuery.length; i++) {
      if (formattedQuery[i] === '?' && paramIndex < parameters.length) {
        const param = parameters[paramIndex];
        const value = this.formatParameter(param);
        formattedQuery = formattedQuery.substring(0, i) + value + formattedQuery.substring(i + 1);
        paramIndex++;
        i += value.length - 1; // Adjust index for the replaced value
      }
    }
    
    return formattedQuery;
  }

  /**
   * Format a parameter value for SQLite
   */
  private formatParameter(param: unknown): string {
    if (param === null || param === undefined) {
      return 'NULL';
    }
    
    if (typeof param === 'string') {
      // Escape single quotes by doubling them
      return `'${param.replace(/'/g, "''")}'`;
    }
    
    if (typeof param === 'number') {
      return String(param);
    }
    
    if (typeof param === 'boolean') {
      return param ? '1' : '0';
    }
    
    if (param instanceof Date) {
      return `'${param.toISOString()}'`;
    }
    
    // For other types, convert to string and escape
    return `'${String(param).replace(/'/g, "''")}'`;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`SQLite error: ${String(error)}`);
  }
}