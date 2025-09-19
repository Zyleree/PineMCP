import { BaseDatabaseAdapter } from './base-database-adapter.js';
import { DatabaseConfig, DatabaseType } from '../types/database.js';
import { DatabaseAdapterFactory } from './database-adapter-factory.js';
import { ConnectionError } from '../types/errors.js';

export class DatabaseConnectionManager {
  private connections: Map<string, BaseDatabaseAdapter> = new Map();
  private currentConnection: string | null = null;

  async addConnection(name: string, config: DatabaseConfig): Promise<void> {
    try {
      const database = DatabaseAdapterFactory.createDatabase(config);
      await database.connect();
      this.connections.set(name, database);
      
      if (!this.currentConnection) {
        this.currentConnection = name;
      }
    } catch (error) {
      throw new ConnectionError(`Failed to add connection '${name}': ${error}`, name);
    }
  }

  async removeConnection(name: string): Promise<void> {
    const connection = this.connections.get(name);
    if (connection) {
      await connection.disconnect();
      this.connections.delete(name);
      
      if (this.currentConnection === name) {
        this.currentConnection = this.connections.keys().next().value || null;
      }
    }
  }

  getConnection(name?: string): BaseDatabaseAdapter | null {
    if (name) {
      return this.connections.get(name) || null;
    }
    return this.currentConnection ? this.connections.get(this.currentConnection) || null : null;
  }

  setCurrentConnection(name: string): void {
    if (this.connections.has(name)) {
      this.currentConnection = name;
    } else {
      throw new ConnectionError(`Connection '${name}' not found`, name);
    }
  }

  getCurrentConnectionName(): string | null {
    return this.currentConnection;
  }

  listConnections(): Array<{ name: string; type: DatabaseType; connected: boolean }> {
    const result: Array<{ name: string; type: DatabaseType; connected: boolean }> = [];

    for (const [name, connection] of this.connections) {
      result.push({
        name,
        type: connection.getType() as DatabaseType,
        connected: connection.isConnected(),
      });
    }

    return result;
  }

  async validateAllConnections(): Promise<Record<string, boolean>> {
    const results: Record<string, boolean> = {};
    
    for (const [name, connection] of this.connections) {
      try {
        results[name] = await connection.validateConnection();
      } catch {
        results[name] = false;
      }
    }
    
    return results;
  }

  async disconnectAll(): Promise<void> {
    for (const [_name, connection] of this.connections) {
      try {
        await connection.disconnect();
      } catch (error) {
        // Continue with cleanup even if disconnect fails
        console.error('Failed to disconnect:', error);
      }
    }
    this.connections.clear();
    this.currentConnection = null;
  }

  hasConnection(name: string): boolean {
    return this.connections.has(name);
  }

  getConnectionCount(): number {
    return this.connections.size;
  }
}
