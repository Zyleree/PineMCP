import { DynamoDBClient, ListTablesCommand, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand as DocScanCommand, QueryCommand as DocQueryCommand } from '@aws-sdk/lib-dynamodb';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats, FieldInfo } from '../types/database.js';
import { ConnectionError, QueryError } from '../types/errors.js';

export class DynamoDBAdapter extends BaseDatabaseAdapter {
  private client: DynamoDBClient | null = null;
  private docClient: DynamoDBDocumentClient | null = null;

  async connect(): Promise<void> {
    const config = this.config;
    const clientConfig: any = {
      region: config.region || 'us-east-1',
    };

    if (config.accessKeyId && config.secretAccessKey) {
      clientConfig.credentials = {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      };
    }

    if (config.endpoint) {
      clientConfig.endpoint = config.endpoint;
    }

    this.client = new DynamoDBClient(clientConfig);
    this.docClient = DynamoDBDocumentClient.from(this.client);
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.docClient = null;
    }
  }

  async executeQuery(query: string, _parameters?: unknown[]): Promise<QueryResult> {
    if (!this.docClient) {
      throw new ConnectionError('Not connected to database', 'dynamodb');
    }

    try {
      const queryObj = JSON.parse(query);
      
      if (queryObj.operation === 'scan') {
        const command = new DocScanCommand({
          TableName: queryObj.tableName,
          FilterExpression: queryObj.filterExpression,
          ExpressionAttributeNames: queryObj.expressionAttributeNames,
          ExpressionAttributeValues: queryObj.expressionAttributeValues,
          Limit: queryObj.limit,
        });

        const result = await this.docClient.send(command);
        
        return {
          rows: result.Items || [],
          rowCount: result.Count || 0,
          fields: this.mapFields(result.Items?.[0] || {}),
        };
      } else if (queryObj.operation === 'query') {
        const command = new DocQueryCommand({
          TableName: queryObj.tableName,
          KeyConditionExpression: queryObj.keyConditionExpression,
          FilterExpression: queryObj.filterExpression,
          ExpressionAttributeNames: queryObj.expressionAttributeNames,
          ExpressionAttributeValues: queryObj.expressionAttributeValues,
          Limit: queryObj.limit,
        });

        const result = await this.docClient.send(command);
        
        return {
          rows: result.Items || [],
          rowCount: result.Count || 0,
          fields: this.mapFields(result.Items?.[0] || {}),
        };
      } else {
        throw new QueryError('Unsupported operation. Use "scan" or "query"', 'dynamodb', query);
      }
    } catch (error) {
      throw new QueryError(`Query execution failed: ${error instanceof Error ? error.message : 'Unknown error'}`, 'dynamodb', query);
    }
  }

  async getTables(): Promise<TableInfo[]> {
    if (!this.client) {
      throw new ConnectionError('Not connected to database', 'dynamodb');
    }

    const command = new ListTablesCommand({});
    const result = await this.client.send(command);
    const tableNames = result.TableNames || [];
    
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
          type: 'table',
          columns: [],
          indexes: [],
          constraints: [],
        });
      }
    }
    
    return tables;
  }

  async getTableInfo(tableName: string, _schema?: string): Promise<TableInfo | null> {
    if (!this.client) {
      throw new ConnectionError('Not connected to database', 'dynamodb');
    }

    const command = new DescribeTableCommand({ TableName: tableName });
    const result = await this.client.send(command);
    const table = result.Table;

    if (!table) {
      throw new QueryError(`Table ${tableName} not found`, 'dynamodb');
    }

    // Get attribute definitions
    const columns = table.AttributeDefinitions?.map((attr: any) => ({
      name: attr.AttributeName || '',
      dataType: this.mapDynamoDBType(attr.AttributeType || 'S'),
      nullable: true, // DynamoDB doesn't have nullable concept
      defaultValue: undefined,
      isPrimaryKey: this.isPrimaryKey(attr.AttributeName || '', table.KeySchema || []),
      isForeignKey: false, // DynamoDB doesn't have foreign keys
      maxLength: undefined,
      precision: undefined,
      scale: undefined,
    })) || [];

    // Get global secondary indexes
    const indexes = table.GlobalSecondaryIndexes?.map((gsi: any) => ({
      name: gsi.IndexName || '',
      columns: gsi.KeySchema?.map((key: any) => key.AttributeName || '') || [],
      unique: false,
      type: 'GSI',
    })) || [];

    // Get local secondary indexes
    const localIndexes = table.LocalSecondaryIndexes?.map((lsi: any) => ({
      name: lsi.IndexName || '',
      columns: lsi.KeySchema?.map((key: any) => key.AttributeName || '') || [],
      unique: false,
      type: 'LSI',
    })) || [];

    return {
      name: tableName,
      type: 'table',
      columns,
      indexes: [...indexes, ...localIndexes],
      constraints: [], // DynamoDB doesn't have traditional constraints
    };
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    if (!this.client) {
      throw new ConnectionError('Not connected to database', 'dynamodb');
    }

    const tables = await this.getTables();
    let totalSize = 0;
    let totalIndexes = 0;

    for (const table of tables) {
      try {
        const command = new DescribeTableCommand({ TableName: table.name });
        const result = await this.client.send(command);
        const tableInfo = result.Table;

        if (tableInfo) {
          totalSize += tableInfo.TableSizeBytes || 0;
          totalIndexes += (tableInfo.GlobalSecondaryIndexes?.length || 0) + (tableInfo.LocalSecondaryIndexes?.length || 0);
        }
      } catch (error) {
        // Skip tables that can't be described
        continue;
      }
    }

    return {
      totalTables: tables.length,
      totalViews: 0, // DynamoDB doesn't have views
      totalIndexes,
      databaseSize: `${(totalSize / 1024 / 1024).toFixed(2)} MB`,
      connectionCount: 1, // DynamoDB doesn't expose connection count
    };
  }

  async validateConnection(): Promise<boolean> {
    try {
      await this.getTables();
      return true;
    } catch {
      return false;
    }
  }

  isConnected(): boolean {
    return this.connected && this.client !== null;
  }

  protected formatQuery(query: string, _parameters?: unknown[]): string {
    // DynamoDB uses JSON format, so we don't need to format parameters
    return query;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`DynamoDB error: ${String(error)}`);
  }

  async beginTransaction(): Promise<void> {
    // DynamoDB doesn't support traditional transactions in the same way
    // Transactions are handled at the item level
  }

  async commitTransaction(): Promise<void> {
    // DynamoDB doesn't support traditional transactions in the same way
  }

  async rollbackTransaction(): Promise<void> {
    // DynamoDB doesn't support traditional transactions in the same way
  }

  isInTransaction(): boolean {
    return false; // DynamoDB doesn't support traditional transactions
  }

  private mapFields(item: Record<string, any>): FieldInfo[] {
    return Object.keys(item).map(key => ({
      name: key,
      dataType: this.getDynamoDBValueType(item[key]),
      nullable: true,
      defaultValue: undefined,
    }));
  }

  private mapDynamoDBType(type: string): string {
    switch (type) {
      case 'S': return 'String';
      case 'N': return 'Number';
      case 'B': return 'Binary';
      case 'BOOL': return 'Boolean';
      case 'SS': return 'StringSet';
      case 'NS': return 'NumberSet';
      case 'BS': return 'BinarySet';
      case 'L': return 'List';
      case 'M': return 'Map';
      case 'NULL': return 'Null';
      default: return 'Unknown';
    }
  }

  private getDynamoDBValueType(value: any): string {
    if (typeof value === 'string') return 'String';
    if (typeof value === 'number') return 'Number';
    if (typeof value === 'boolean') return 'Boolean';
    if (Array.isArray(value)) return 'List';
    if (value && typeof value === 'object') return 'Map';
    if (value === null) return 'Null';
    return 'Unknown';
  }

  private isPrimaryKey(attributeName: string, keySchema: any[]): boolean {
    return keySchema.some(key => key.AttributeName === attributeName);
  }
}
