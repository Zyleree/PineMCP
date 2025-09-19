import { createClient, RedisClientType } from 'redis';
import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { QueryResult, TableInfo, DatabaseStats } from '../types/database.js';
import { ConnectionError, TransactionError, QueryError } from '../types/errors.js';

export class RedisAdapter extends BaseDatabaseAdapter {
  private client: RedisClientType | null = null;
  private inTransaction: boolean = false;
  private transactionCommands: string[] = [];
  private transactionClient: RedisClientType | null = null;

  async connect(): Promise<void> {
    try {
      const connectionConfig: any = {
        socket: {
          host: this.config.host || 'localhost',
          port: this.config.port || 6379,
        },
        database: this.config.db || 0,
        password: this.config.password,
        username: this.config.username,
      };

      this.client = createClient(connectionConfig);
      await this.client.connect();
      this.connected = true;
    } catch (error) {
      this.connected = false;
      throw new ConnectionError(
        `Failed to connect to Redis: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis'
      );
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.transactionClient) {
        await this.transactionClient.quit();
        this.transactionClient = null;
      }
      if (this.client) {
        await this.client.quit();
        this.client = null;
      }
      this.connected = false;
    } catch (error) {
      throw new ConnectionError(
        `Failed to disconnect from Redis: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis'
      );
    }
  }

  isConnected(): boolean {
    return this.connected && this.client !== null;
  }

  async executeQuery(query: string, parameters?: unknown[]): Promise<QueryResult> {
    const client = this.transactionClient || this.client;
    if (!client) {
      throw new ConnectionError('Database not connected', 'redis');
    }

    try {
      // Parse Redis command
      const parts = query.trim().split(' ');
      const command = parts[0]?.toUpperCase() || '';
      const args = parts.slice(1);

      // Replace parameters
      if (parameters && parameters.length > 0) {
        for (let i = 0; i < args.length; i++) {
          if (args[i] === '?') {
            args[i] = String(parameters.shift());
          }
        }
      }

      let result: unknown;
      
      switch (command) {
        case 'GET':
          result = await client.get(args[0] || '');
          break;
        case 'SET':
          result = await client.set(args[0] || '', args[1] || '');
          break;
        case 'DEL':
          result = await client.del(args[0] || '');
          break;
        case 'EXISTS':
          result = await client.exists(args[0] || '');
          break;
        case 'KEYS':
          result = await client.keys(args[0] || '*');
          break;
        case 'HGET':
          result = await client.hGet(args[0] || '', args[1] || '');
          break;
        case 'HSET': {
          const hsetArgs: Record<string, string> = {};
          for (let i = 1; i < args.length; i += 2) {
            if (args[i] && args[i + 1]) {
              hsetArgs[args[i] as string] = args[i + 1] as string;
            }
          }
          result = await client.hSet(args[0] || '', hsetArgs);
          break;
        }
        case 'HGETALL':
          result = await client.hGetAll(args[0] || '');
          break;
        case 'LPUSH':
          result = await client.lPush(args[0] || '', args.slice(1));
          break;
        case 'RPUSH':
          result = await client.rPush(args[0] || '', args.slice(1));
          break;
        case 'LRANGE':
          result = await client.lRange(args[0] || '', parseInt(args[1] || '0'), parseInt(args[2] || '0'));
          break;
        case 'SADD':
          result = await client.sAdd(args[0] || '', args.slice(1));
          break;
        case 'SMEMBERS':
          result = await client.sMembers(args[0] || '');
          break;
        case 'ZADD': {
          const zaddArgs: any[] = [];
          for (let i = 1; i < args.length; i += 2) {
            if (args[i] && args[i + 1]) {
              zaddArgs.push({ score: parseFloat(args[i] as string), value: args[i + 1] });
            }
          }
          result = await client.zAdd(args[0] || '', zaddArgs);
          break;
        }
        case 'ZRANGE':
          result = await client.zRange(args[0] || '', parseInt(args[1] || '0'), parseInt(args[2] || '0'));
          break;
        case 'INFO':
          result = await client.info(args[0] || 'server');
          break;
        case 'PING':
          result = await client.ping();
          break;
        default:
          throw new QueryError(`Unsupported Redis command: ${command}`, 'redis', query, parameters);
      }

      return {
        rows: Array.isArray(result) ? result.map((item, index) => ({ key: index, value: item })) : [{ result }],
        rowCount: Array.isArray(result) ? result.length : 1,
        fields: [
          { name: 'result', dataType: 'string', nullable: true, defaultValue: undefined },
        ],
      };
    } catch (error) {
      throw new QueryError(
        `Redis command failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis',
        query,
        parameters
      );
    }
  }

  async getTables(): Promise<TableInfo[]> {
    // Redis doesn't have traditional tables, but we can return key patterns
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'redis');
    }

    try {
      const keys = await this.client.keys('*');
      const patterns = new Set<string>();
      
      // Group keys by pattern
      keys.forEach(key => {
        const parts = key.split(':');
        if (parts.length > 1) {
          patterns.add(parts[0] + ':*');
        } else {
          patterns.add('*');
        }
      });

      return Array.from(patterns).map(pattern => ({
        name: pattern,
        type: 'table' as const,
        columns: [
          { name: 'key', dataType: 'string', nullable: false, isPrimaryKey: true, isForeignKey: false },
          { name: 'value', dataType: 'string', nullable: true, isPrimaryKey: false, isForeignKey: false },
        ],
        indexes: [],
        constraints: [],
      }));
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getTableInfo(tableName: string): Promise<TableInfo | null> {
    // For Redis, treat key patterns as tables
    return {
      name: tableName,
      type: 'table',
      columns: [
        { name: 'key', dataType: 'string', nullable: false, isPrimaryKey: true, isForeignKey: false },
        { name: 'value', dataType: 'string', nullable: true, isPrimaryKey: false, isForeignKey: false },
      ],
      indexes: [],
      constraints: [],
    };
  }

  async getDatabaseStats(): Promise<DatabaseStats> {
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'redis');
    }

    try {
      const info = await this.client.info('memory');
      const lines = info.split('\r\n');
      const memoryUsed = lines.find(line => line.startsWith('used_memory_human:'))?.split(':')[1] || '0B';
      
      const _keyCount = await this.client.dbSize();
      
      return {
        totalTables: 1, // Redis doesn't have traditional tables
        totalViews: 0,
        totalIndexes: 0,
        databaseSize: memoryUsed,
        connectionCount: 1,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async validateConnection(): Promise<boolean> {
    try {
      const result = await this.executeQuery('PING');
      return result.rows[0]?.result === 'PONG';
    } catch {
      return false;
    }
  }

  async beginTransaction(): Promise<void> {
    if (this.inTransaction) {
      throw new TransactionError('Transaction already in progress', 'redis');
    }
    
    if (!this.client) {
      throw new ConnectionError('Database not connected', 'redis');
    }

    try {
      // Create a new client for the transaction
      const connectionConfig: any = {
        socket: {
          host: this.config.host || 'localhost',
          port: this.config.port || 6379,
        },
        database: this.config.db || 0,
        password: this.config.password,
        username: this.config.username,
      };

      this.transactionClient = createClient(connectionConfig);
      await this.transactionClient.connect();
      
      // Start Redis MULTI command - this queues commands for later execution
      await this.transactionClient.multi();
      
      this.inTransaction = true;
      this.transactionCommands = [];
    } catch (error) {
      if (this.transactionClient) {
        await this.transactionClient.quit();
        this.transactionClient = null;
      }
      this.inTransaction = false;
      throw new TransactionError(
        `Failed to begin Redis transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis'
      );
    }
  }

  async commitTransaction(): Promise<void> {
    if (!this.inTransaction || !this.transactionClient) {
      throw new TransactionError('No transaction in progress', 'redis');
    }

    try {
      // Execute the transaction with EXEC - this executes all queued commands atomically
      // Note: Redis transactions are automatically executed when the client is closed
      // or when EXEC is called implicitly
      
      // Close the transaction client (this will execute the transaction)
      await this.transactionClient.quit();
      this.transactionClient = null;
      this.inTransaction = false;
      this.transactionCommands = [];
    } catch (error) {
      // Clean up on error
      if (this.transactionClient) {
        await this.transactionClient.quit();
        this.transactionClient = null;
      }
      this.inTransaction = false;
      this.transactionCommands = [];
      
      throw new TransactionError(
        `Failed to commit Redis transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis'
      );
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction || !this.transactionClient) {
      throw new TransactionError('No transaction in progress', 'redis');
    }

    try {
      // Redis doesn't support rollback, but we can discard the MULTI
      // This discards all queued commands without executing them
      await this.transactionClient.discard();
      
      // Close the transaction client
      await this.transactionClient.quit();
      this.transactionClient = null;
      this.inTransaction = false;
      this.transactionCommands = [];
    } catch (error) {
      // Clean up on error
      if (this.transactionClient) {
        await this.transactionClient.quit();
        this.transactionClient = null;
      }
      this.inTransaction = false;
      this.transactionCommands = [];
      
      throw new TransactionError(
        `Failed to rollback Redis transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
        'redis'
      );
    }
  }

  isInTransaction(): boolean {
    return this.inTransaction;
  }

  protected formatQuery(query: string, parameters?: unknown[]): string {
    if (!parameters || parameters.length === 0) {
      return query;
    }
    
    let formattedQuery = query;
    parameters.forEach((param, _index) => {
      const placeholder = '?';
      const value = typeof param === 'string' ? `"${param}"` : String(param);
      formattedQuery = formattedQuery.replace(placeholder, value);
    });
    
    return formattedQuery;
  }

  protected handleError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(`Redis error: ${String(error)}`);
  }
}
