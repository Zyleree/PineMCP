import { DatabaseConfig, QueryResult, TableInfo, DatabaseStats, DatabaseOperation } from '../types/database.js';

export abstract class BaseDatabaseAdapter {
  protected config: DatabaseConfig;
  protected connected: boolean = false;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract isConnected(): boolean;
  abstract executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult>;
  abstract getTables(): Promise<TableInfo[]>;
  abstract getTableInfo(tableName: string, schema?: string): Promise<TableInfo | null>;
  abstract getDatabaseStats(): Promise<DatabaseStats>;
  abstract validateConnection(): Promise<boolean>;
  
  protected abstract formatQuery(query: string, parameters?: unknown[]): string;
  protected abstract handleError(error: unknown): Error;
  
  protected validateQuery(query: string): void {
    if (!query || typeof query !== 'string') {
      throw new Error('Query must be a non-empty string');
    }
    
    const dangerousPatterns = [
      /;\s*drop\s+table/i,
      /;\s*delete\s+from/i,
      /;\s*truncate\s+table/i,
      /;\s*alter\s+table/i,
      /;\s*drop\s+database/i,
    ];
    
    for (const pattern of dangerousPatterns) {
      if (pattern.test(query)) {
        throw new Error('Potentially dangerous query detected');
      }
    }
  }

  protected validateParameters(parameters?: unknown[]): void {
    if (parameters && !Array.isArray(parameters)) {
      throw new Error('Parameters must be an array');
    }
  }

  abstract beginTransaction(): Promise<void>;
  abstract commitTransaction(): Promise<void>;
  abstract rollbackTransaction(): Promise<void>;
  abstract isInTransaction(): boolean;
  
  async ensureConnection(): Promise<void> {
    if (!this.isConnected()) {
      await this.connect();
    }
  }

  async safeExecuteQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    try {
      this.validateQuery(query);
      this.validateParameters(parameters);
      await this.ensureConnection();
      return await this.executeQuery(query, parameters);
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async executeBatch(operations: DatabaseOperation[]): Promise<QueryResult[]> {
    const results: QueryResult[] = [];
    
    try {
      await this.beginTransaction();
      
      for (const operation of operations) {
        const result = await this.executeQuery(operation.query, operation.parameters);
        results.push(result);
      }
      
      await this.commitTransaction();
      return results;
    } catch (error) {
      await this.rollbackTransaction();
      throw this.handleError(error);
    }
  }
}
