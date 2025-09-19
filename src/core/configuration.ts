import { readFileSync, existsSync, writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { MCPConfig } from '../types/mcp.js';
import { DatabaseConfig } from '../types/database.js';
import { DatabaseAdapterFactory } from '../adapters/database-adapter-factory.js';
import { ConfigurationError } from '../types/errors.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export class Configuration {
  private static readonly CONFIG_PATH = join(__dirname, '../../config/mcp-config.json');
  private static readonly DEFAULT_CONFIG_PATH = join(__dirname, '../../config/mcp-config.default.json');

  static load(): MCPConfig {
    const empty: MCPConfig = {
      databases: [],
      server: {
        name: '',
        version: '',
        description: '',
      },
      logging: {
        level: 'info',
        format: 'text',
      },
    } as MCPConfig;
    return this.mergeDiscoveredMCPJsonDatabases(empty);
  }

  static save(config: MCPConfig): void {
    try {
      const configDir = join(__dirname, '../../config');
      if (!existsSync(configDir)) {
        mkdirSync(configDir, { recursive: true });
      }
      
      const configPath = join(configDir, 'mcp-config.json');
      const configData = JSON.stringify(config, null, 2);
      writeFileSync(configPath, configData, 'utf8');
    } catch (error) {
      throw new ConfigurationError(
        `Failed to save configuration: ${error instanceof Error ? error.message : 'Unknown error'}`,
        join(__dirname, '../../config/mcp-config.json')
      );
    }
  }


  static validateDatabaseConfig(config: DatabaseConfig): { valid: boolean; errors: string[] } {
    return DatabaseAdapterFactory.validateConfig(config);
  }

  static parseDatabaseUrl(url: string): DatabaseConfig | null {
    try {
      const parsedUrl = new URL(url);
      switch (parsedUrl.protocol) {
        case 'postgresql:':
        case 'postgres:':
          return {
            type: 'postgresql',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 5432,
            database: parsedUrl.pathname.slice(1),
            username: parsedUrl.username,
            password: parsedUrl.password,
            ssl: parsedUrl.searchParams.get('sslmode') === 'require',
          };
        case 'mysql:':
          return {
            type: 'mysql',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 3306,
            database: parsedUrl.pathname.slice(1),
            username: parsedUrl.username,
            password: parsedUrl.password,
            ssl: parsedUrl.searchParams.get('ssl') === 'true',
          };
        case 'sqlite:':
          return {
            type: 'sqlite',
            filename: parsedUrl.pathname,
          } as DatabaseConfig;
        case 'redis:':
          return {
            type: 'redis',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 6379,
            db: parsedUrl.pathname.slice(1) ? parseInt(parsedUrl.pathname.slice(1)) : 0,
            password: parsedUrl.password,
          } as DatabaseConfig;
        case 'mongodb:':
          return {
            type: 'mongodb',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 27017,
            database: parsedUrl.pathname.slice(1),
            username: parsedUrl.username,
            password: parsedUrl.password,
            authSource: parsedUrl.searchParams.get('authSource') || undefined,
          } as DatabaseConfig;
        case 'cassandra:':
          return {
            type: 'cassandra',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 9042,
            keyspace: parsedUrl.pathname.slice(1),
            username: parsedUrl.username,
            password: parsedUrl.password,
            datacenter: parsedUrl.searchParams.get('datacenter') || 'datacenter1',
          } as DatabaseConfig;
        case 'mssql:':
          return {
            type: 'mssql',
            host: parsedUrl.hostname,
            port: parsedUrl.port ? parseInt(parsedUrl.port) : 1433,
            database: parsedUrl.pathname.slice(1),
            username: parsedUrl.username,
            password: parsedUrl.password,
          } as DatabaseConfig;
        case 'dynamodb:':
          return {
            type: 'dynamodb',
            region: parsedUrl.searchParams.get('region') || undefined,
            endpoint: parsedUrl.origin !== 'null' ? `${parsedUrl.protocol}//${parsedUrl.host}` : undefined,
          } as DatabaseConfig;
        default:
          return null;
      }
    } catch (error) {
      return null;
    }
  }


  private static mergeDiscoveredMCPJsonDatabases(config: MCPConfig): MCPConfig {
    const discovered: Array<{ name: string; cfg: DatabaseConfig }> = [];
    const mcpPaths = this.getPotentialMCPConfigPaths();
    for (const path of mcpPaths) {
      try {
        if (!existsSync(path)) continue;
        const raw = readFileSync(path, 'utf-8');
        const json = JSON.parse(raw);
        if (!json || !json.mcpServers) continue;
        const servers = json.mcpServers as Record<string, any>;
        for (const [_name, server] of Object.entries(servers)) {
          if (!server) continue;
          const arrays = [server.connections, server.databases, server.dbUrls, server.databaseUrls, server.database_urls, server.db_urls];
          for (const arr of arrays) {
            if (!arr) continue;
            if (Array.isArray(arr)) {
              arr.forEach((entry: any, index: number) => {
                if (typeof entry === 'string') {
                  const parsed = this.parseDatabaseUrl(entry);
                  if (parsed) {
                    const name = this.deriveConnectionNameFromUrl(entry, parsed, `mcp-db-${index + 1}`);
                    discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                  }
                } else if (entry && typeof entry === 'object') {
                  if (entry.url && typeof entry.url === 'string') {
                    const parsed = this.parseDatabaseUrl(entry.url);
                    if (parsed) {
                      const name = (entry.name && String(entry.name)) || this.deriveConnectionNameFromUrl(entry.url, parsed, `mcp-db-${index + 1}`);
                      discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                    }
                  } else if (entry.type) {
                    const name = (entry.name && String(entry.name)) || `mcp-db-${index + 1}`;
                    const cfg = { ...entry, name } as DatabaseConfig;
                    discovered.push({ name, cfg });
                  }
                }
              });
              continue;
            }
            if (!Array.isArray(arr) && typeof arr === 'object') {
              const entries = Object.entries(arr as Record<string, any>);
              entries.forEach(([key, value], index) => {
                if (typeof value === 'string') {
                  const parsed = this.parseDatabaseUrl(value);
                  if (parsed) {
                    const name = this.normalizeName(key) || this.deriveConnectionNameFromUrl(value, parsed, `mcp-db-${index + 1}`);
                    discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                  }
                } else if (value && typeof value === 'object') {
                  if (value.url && typeof value.url === 'string') {
                    const parsed = this.parseDatabaseUrl(value.url);
                    if (parsed) {
                      const name = (value.name && String(value.name)) || this.normalizeName(key) || this.deriveConnectionNameFromUrl(value.url, parsed, `mcp-db-${index + 1}`);
                      discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                    }
                  } else if (value.type) {
                    const name = (value.name && String(value.name)) || this.normalizeName(key) || `mcp-db-${index + 1}`;
                    const cfg = { ...value, name } as DatabaseConfig;
                    discovered.push({ name, cfg });
                  }
                }
              });
              continue;
            }
            const list: any[] = Array.isArray(arr)
              ? arr
              : (typeof arr === 'string' ? (arr as string).split(',').map(s => s.trim()).filter(Boolean) : []);
            list.forEach((entry: any, index: number) => {
              if (typeof entry === 'string') {
                const parsed = this.parseDatabaseUrl(entry);
                if (parsed) {
                  const name = this.deriveConnectionNameFromUrl(entry, parsed, `mcp-db-${index + 1}`);
                  discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                }
              } else if (entry && typeof entry === 'object') {
                if (entry.url && typeof entry.url === 'string') {
                  const parsed = this.parseDatabaseUrl(entry.url);
                  if (parsed) {
                    const name = (entry.name && String(entry.name)) || this.deriveConnectionNameFromUrl(entry.url, parsed, `mcp-db-${index + 1}`);
                    discovered.push({ name, cfg: { name, ...parsed } as DatabaseConfig });
                  }
                } else if (entry.type) {
                  const name = (entry.name && String(entry.name)) || `mcp-db-${index + 1}`;
                  const cfg = { ...entry, name } as DatabaseConfig;
                  discovered.push({ name, cfg });
                }
              }
            });
          }
        }
      } catch {
        // Ignore errors when parsing MCP config files
      }
    }
    if (discovered.length === 0) return config;
    const existingNames = new Set(config.databases.map(d => d.name));
    const merged = [
      ...config.databases,
      ...discovered
        .filter(d => !existingNames.has(d.name))
        .map(d => ({ name: d.name, ...d.cfg } as DatabaseConfig)),
    ];
    return { ...config, databases: merged } as MCPConfig;
  }

  private static getPotentialMCPConfigPaths(): string[] {
    const paths: string[] = [];
    const cwd = process.cwd();
    const home = process.env.HOME || process.env.USERPROFILE || '';

    const add = (p?: string) => { if (p) paths.push(p); };

    const envSingle = process.env.PINEMCP_MCP_JSON;
    if (envSingle) add(envSingle);
    const envList = process.env.PINEMCP_MCP_PATHS;
    if (envList) envList.split(',').map(s => s.trim()).filter(Boolean).forEach(p => add(p));

    add(join(cwd, '.cursor', 'mcp.json'));
    add(join(cwd, '.vscode', 'mcp.json'));
    add(join(cwd, 'mcp.json'));

    if (home) {
      add(join(home, '.cursor', 'mcp.json'));
      add(join(home, '.vscode', 'mcp.json'));
      add(join(home, '.mcp', 'mcp.json'));
    }

    if (process.platform === 'win32' && process.env.APPDATA) {
      add(join(process.env.APPDATA, 'Cursor', 'User', 'globalStorage', 'cursor.mcp', 'mcp.json'));
    }
    if (home) {
      add(join(home, 'Library', 'Application Support', 'Cursor', 'User', 'globalStorage', 'cursor.mcp', 'mcp.json'));
      add(join(home, '.config', 'Cursor', 'User', 'globalStorage', 'cursor.mcp', 'mcp.json'));
    }

    return paths;
  }

  private static deriveConnectionNameFromUrl(url: string, parsed: DatabaseConfig, fallback: string): string {
    try {
      const u = new URL(url);
      const host = u.hostname || 'localhost';
      const dbName = (u.pathname || '').replace(/^\//, '') || (parsed as Record<string, unknown>)['database'] || (parsed as Record<string, unknown>)['keyspace'] || '';
      const scheme = (u.protocol || '').replace(':', '') || (parsed as Record<string, unknown>)['type'];
      const base = dbName ? `${scheme}-${host}-${dbName}` : `${scheme}-${host}`;
      return base.toLowerCase();
    } catch {
      return fallback;
    }
  }

  private static normalizeName(raw: string): string {
    return raw.toLowerCase().replace(/[^a-z0-9-_]/g, '-');
  }
}
