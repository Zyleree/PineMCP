import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { BaseDatabaseAdapter } from '../adapters/base-database-adapter.js';
import { DatabaseAdapterFactory } from '../adapters/database-adapter-factory.js';
import { DatabaseConnectionManager } from '../adapters/database-connection-manager.js';
import { MCPConfig } from '../types/mcp.js';
import { SchemaManagementService } from './schema-management-service.js';
import { DataExportImportService } from './data-export-import-service.js';
import { QueryAnalysisService } from './query-analysis-service.js';
import { ConnectionError, QueryError, ValidationError } from '../types/errors.js';

export class MCPServerService {
  private server: Server;
  private connectionManager: DatabaseConnectionManager;
  private config: MCPConfig;
  private schemaService: SchemaManagementService;
  private exportImportService: DataExportImportService;
  private queryAnalysisService: QueryAnalysisService;

  constructor(config: MCPConfig) {
    this.config = config;
    this.connectionManager = new DatabaseConnectionManager();
    this.schemaService = new SchemaManagementService(this.connectionManager);
    this.exportImportService = new DataExportImportService(this.connectionManager);
    this.queryAnalysisService = new QueryAnalysisService(this.connectionManager);
    this.server = new Server(
      {
        name: config.server.name,
        version: config.server.version,
      }
    );

    this.setupHandlers();
  }

  private setupHandlers(): void {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: 'execute_query',
            description: 'Execute a query on the current database connection',
            inputSchema: {
              type: 'object',
              properties: {
                query: {
                  type: 'string',
                  description: 'Query to execute (SQL for relational DBs, JSON for MongoDB, commands for Redis)',
                },
                parameters: {
                  type: 'array',
                  description: 'Query parameters',
                  items: { type: 'string' },
                },
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
              required: ['query'],
            },
          },
          {
            name: 'get_tables',
            description: 'Get list of all tables/collections in the current database',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'get_table_info',
            description: 'Get detailed information about a specific table/collection',
            inputSchema: {
              type: 'object',
              properties: {
                table_name: {
                  type: 'string',
                  description: 'Name of the table/collection',
                },
                schema: {
                  type: 'string',
                  description: 'Schema name (optional)',
                },
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
              required: ['table_name'],
            },
          },
          {
            name: 'get_database_stats',
            description: 'Get database statistics and information',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'validate_connection',
            description: 'Validate database connection',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'begin_transaction',
            description: 'Begin a database transaction',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'commit_transaction',
            description: 'Commit the current transaction',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'rollback_transaction',
            description: 'Rollback the current transaction',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
            },
          },
          {
            name: 'execute_batch',
            description: 'Execute multiple queries in a transaction',
            inputSchema: {
              type: 'object',
              properties: {
                operations: {
                  type: 'array',
                  description: 'Array of database operations',
                  items: {
                    type: 'object',
                    properties: {
                      type: {
                        type: 'string',
                        enum: ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'],
                      },
                      query: { type: 'string' },
                      parameters: {
                        type: 'array',
                        items: { type: 'string' },
                      },
                    },
                    required: ['type', 'query'],
                  },
                },
                connection: {
                  type: 'string',
                  description: 'Connection name (optional, uses current if not specified)',
                },
              },
              required: ['operations'],
            },
          },
          {
            name: 'add_connection',
            description: 'Add a new database connection',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Name for the connection',
                },
                config: {
                  type: 'object',
                  description: 'Database configuration',
                  properties: {
                    type: {
                      type: 'string',
                      enum: ['postgresql', 'mysql', 'sqlite', 'redis', 'mongodb', 'cassandra', 'mssql', 'dynamodb'],
                    },
                    host: { type: 'string' },
                    port: { type: 'number' },
                    database: { type: 'string' },
                    username: { type: 'string' },
                    password: { type: 'string' },
                    ssl: { type: 'boolean' },
                    filename: { type: 'string' },
                    db: { type: 'number' },
                    authSource: { type: 'string' },
                    keyspace: { type: 'string' },
                    datacenter: { type: 'string' },
                  },
                  required: ['type'],
                },
              },
              required: ['name', 'config'],
            },
          },
          {
            name: 'remove_connection',
            description: 'Remove a database connection',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Name of the connection to remove',
                },
              },
              required: ['name'],
            },
          },
          {
            name: 'list_connections',
            description: 'List all database connections',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'switch_connection',
            description: 'Switch to a different database connection',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Name of the connection to switch to',
                },
              },
              required: ['name'],
            },
          },
          {
            name: 'get_current_connection',
            description: 'Get the current active connection name',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'compare_schemas',
            description: 'Compare schemas between two database connections',
            inputSchema: {
              type: 'object',
              properties: {
                source_connection: {
                  type: 'string',
                  description: 'Source connection name',
                },
                target_connection: {
                  type: 'string',
                  description: 'Target connection name',
                },
              },
              required: ['source_connection', 'target_connection'],
            },
          },
          {
            name: 'generate_migration',
            description: 'Generate migration script from schema comparison',
            inputSchema: {
              type: 'object',
              properties: {
                source_connection: {
                  type: 'string',
                  description: 'Source connection name',
                },
                target_connection: {
                  type: 'string',
                  description: 'Target connection name',
                },
                migration_name: {
                  type: 'string',
                  description: 'Name for the migration',
                },
              },
              required: ['source_connection', 'target_connection', 'migration_name'],
            },
          },
          {
            name: 'generate_ddl',
            description: 'Generate DDL for a database connection',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
                include_data: {
                  type: 'boolean',
                  description: 'Include data in DDL',
                  default: false,
                },
                include_indexes: {
                  type: 'boolean',
                  description: 'Include indexes in DDL',
                  default: true,
                },
                include_constraints: {
                  type: 'boolean',
                  description: 'Include constraints in DDL',
                  default: true,
                },
                format: {
                  type: 'string',
                  enum: ['sql', 'json', 'yaml'],
                  description: 'Output format',
                  default: 'sql',
                },
              },
              required: ['connection'],
            },
          },
          {
            name: 'validate_schema',
            description: 'Validate schema consistency',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
              },
              required: ['connection'],
            },
          },
          {
            name: 'export_data',
            description: 'Export data from database to file',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
                output_path: {
                  type: 'string',
                  description: 'Output file path',
                },
                format: {
                  type: 'string',
                  enum: ['json', 'csv', 'sql', 'xml'],
                  description: 'Export format',
                },
                tables: {
                  type: 'array',
                  items: { type: 'string' },
                  description: 'Specific tables to export (optional)',
                },
                where_clause: {
                  type: 'string',
                  description: 'WHERE clause for filtering (optional)',
                },
                limit: {
                  type: 'number',
                  description: 'Limit number of records (optional)',
                },
                include_schema: {
                  type: 'boolean',
                  description: 'Include schema information',
                  default: true,
                },
                pretty_print: {
                  type: 'boolean',
                  description: 'Pretty print output',
                  default: false,
                },
              },
              required: ['connection', 'output_path', 'format'],
            },
          },
          {
            name: 'import_data',
            description: 'Import data from file to database',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
                file_path: {
                  type: 'string',
                  description: 'Input file path',
                },
                format: {
                  type: 'string',
                  enum: ['json', 'csv', 'sql', 'xml'],
                  description: 'Import format',
                },
                table_name: {
                  type: 'string',
                  description: 'Target table name',
                },
                mode: {
                  type: 'string',
                  enum: ['insert', 'upsert', 'replace'],
                  description: 'Import mode',
                  default: 'insert',
                },
                batch_size: {
                  type: 'number',
                  description: 'Batch size for import',
                  default: 1000,
                },
                skip_errors: {
                  type: 'boolean',
                  description: 'Skip errors and continue',
                  default: false,
                },
                mapping: {
                  type: 'object',
                  description: 'Column mapping (optional)',
                },
              },
              required: ['connection', 'file_path', 'format', 'table_name'],
            },
          },
          {
            name: 'analyze_query',
            description: 'Analyze query performance and provide recommendations',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
                query: {
                  type: 'string',
                  description: 'Query to analyze',
                },
                parameters: {
                  type: 'array',
                  items: { type: 'string' },
                  description: 'Query parameters',
                },
              },
              required: ['connection', 'query'],
            },
          },
          {
            name: 'get_query_history',
            description: 'Get query execution history',
            inputSchema: {
              type: 'object',
              properties: {
                limit: {
                  type: 'number',
                  description: 'Number of queries to return',
                  default: 50,
                },
              },
            },
          },
          {
            name: 'get_slow_queries',
            description: 'Get slow queries from history',
            inputSchema: {
              type: 'object',
              properties: {
                threshold: {
                  type: 'number',
                  description: 'Execution time threshold in milliseconds',
                  default: 1000,
                },
              },
            },
          },
          {
            name: 'get_query_statistics',
            description: 'Get query performance statistics',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'save_query_template',
            description: 'Save a query template',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Template name',
                },
                description: {
                  type: 'string',
                  description: 'Template description',
                },
                query: {
                  type: 'string',
                  description: 'Query template with {parameter} placeholders',
                },
                parameters: {
                  type: 'array',
                  items: {
                    type: 'object',
                    properties: {
                      name: { type: 'string' },
                      type: { type: 'string' },
                      required: { type: 'boolean' },
                      defaultValue: { type: 'string' },
                    },
                    required: ['name', 'type', 'required'],
                  },
                  description: 'Template parameters',
                },
                tags: {
                  type: 'array',
                  items: { type: 'string' },
                  description: 'Template tags',
                },
                connection_type: {
                  type: 'string',
                  description: 'Database type this template is for',
                },
              },
              required: ['name', 'description', 'query', 'parameters', 'connection_type'],
            },
          },
          {
            name: 'get_query_templates',
            description: 'Get query templates',
            inputSchema: {
              type: 'object',
              properties: {
                connection_type: {
                  type: 'string',
                  description: 'Filter by database type (optional)',
                },
                tags: {
                  type: 'array',
                  items: { type: 'string' },
                  description: 'Filter by tags (optional)',
                },
              },
            },
          },
          {
            name: 'execute_template',
            description: 'Execute a query template with parameters',
            inputSchema: {
              type: 'object',
              properties: {
                connection: {
                  type: 'string',
                  description: 'Connection name',
                },
                template_id: {
                  type: 'string',
                  description: 'Template ID',
                },
                parameters: {
                  type: 'object',
                  description: 'Parameter values',
                },
              },
              required: ['connection', 'template_id', 'parameters'],
            },
          },
        ],
      };
    });

    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      const currentDb = this.connectionManager.getConnection();
      if (!currentDb) {
        return { resources: [] };
      }

      try {
        const tables = await currentDb.getTables();
        return {
          resources: tables.map(table => ({
            uri: `table://${table.schema || 'default'}/${table.name}`,
            name: table.name,
            description: `${table.type} in ${table.schema || 'default'} schema`,
            mimeType: 'application/json',
          })),
        };
      } catch (error) {
        return { resources: [] };
      }
    });

    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      const currentDb = this.connectionManager.getConnection();
      if (!currentDb) {
        throw new ConnectionError('No database connection');
      }

      const uri = request.params.uri;
      if (!uri.startsWith('table://')) {
        throw new ValidationError('Invalid resource URI', 'uri', uri);
      }

      const remainder = uri.slice('table://'.length);
      const parts = remainder.split('/').filter(Boolean);
      let schema: string | undefined = undefined;
      let tableName: string | undefined = undefined;
      if (parts.length === 1) {
        tableName = parts[0];
      } else if (parts.length >= 2) {
        schema = parts[0];
        tableName = parts.slice(1).join('/');
      }
      if (!tableName) {
        throw new ValidationError('Invalid table name in URI', 'tableName', tableName);
      }

      try {
        const tableInfo = await currentDb.getTableInfo(tableName, schema === 'default' ? undefined : schema);
        if (!tableInfo) {
          throw new QueryError('Table not found', undefined, tableName);
        }

        return {
          contents: [
            {
              uri,
              mimeType: 'application/json',
              text: JSON.stringify(tableInfo, null, 2),
            },
          ],
        };
      } catch (error) {
        throw new QueryError(`Failed to read table info: ${error}`, undefined, tableName);
      }
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        switch (name) {
          case 'execute_query':
            return await this.handleExecuteQuery(args);
          case 'get_tables':
            return await this.handleGetTables(args);
          case 'get_table_info':
            return await this.handleGetTableInfo(args);
          case 'get_database_stats':
            return await this.handleGetDatabaseStats(args);
          case 'validate_connection':
            return await this.handleValidateConnection(args);
          case 'begin_transaction':
            return await this.handleBeginTransaction(args);
          case 'commit_transaction':
            return await this.handleCommitTransaction(args);
          case 'rollback_transaction':
            return await this.handleRollbackTransaction(args);
          case 'execute_batch':
            return await this.handleExecuteBatch(args);
          case 'add_connection':
            return await this.handleAddConnection(args);
          case 'remove_connection':
            return await this.handleRemoveConnection(args);
          case 'list_connections':
            return await this.handleListConnections();
          case 'switch_connection':
            return await this.handleSwitchConnection(args);
          case 'get_current_connection':
            return await this.handleGetCurrentConnection();
          case 'compare_schemas':
            return await this.handleCompareSchemas(args);
          case 'generate_migration':
            return await this.handleGenerateMigration(args);
          case 'generate_ddl':
            return await this.handleGenerateDDL(args);
          case 'validate_schema':
            return await this.handleValidateSchema(args);
          case 'export_data':
            return await this.handleExportData(args);
          case 'import_data':
            return await this.handleImportData(args);
          case 'analyze_query':
            return await this.handleAnalyzeQuery(args);
          case 'get_query_history':
            return await this.handleGetQueryHistory(args);
          case 'get_slow_queries':
            return await this.handleGetSlowQueries(args);
          case 'get_query_statistics':
            return await this.handleGetQueryStatistics();
          case 'save_query_template':
            return await this.handleSaveQueryTemplate(args);
          case 'get_query_templates':
            return await this.handleGetQueryTemplates(args);
          case 'execute_template':
            return await this.handleExecuteTemplate(args);
          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `Error: ${error instanceof Error ? error.message : String(error)}`,
            },
          ],
          isError: true,
        };
      }
    });
  }

  private getConnection(connectionName?: string): BaseDatabaseAdapter {
    const connection = this.connectionManager.getConnection(connectionName);
    if (!connection) {
      throw new ConnectionError(`No database connection${connectionName ? ` named '${connectionName}'` : ''} found`, connectionName || 'unknown');
    }
    return connection;
  }

  private async handleExecuteQuery(args: any): Promise<any> {
    const { query, parameters, connection } = args;
    if (!query) {
      throw new QueryError('Query is required');
    }

    const db = this.getConnection(connection);
    const result = await db.safeExecuteQuery(query, parameters);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            rows: result.rows,
            rowCount: result.rowCount,
            fields: result.fields,
          }, null, 2),
        },
      ],
    };
  }

  private async handleGetTables(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    const tables = await db.getTables();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(tables, null, 2),
        },
      ],
    };
  }

  private async handleGetTableInfo(args: any): Promise<any> {
    const { table_name, schema, connection } = args;
    if (!table_name) {
      throw new QueryError('table_name is required');
    }

    const db = this.getConnection(connection);
    const tableInfo = await db.getTableInfo(table_name, schema);
    if (!tableInfo) {
      throw new QueryError(`Table ${table_name} not found`, connection, table_name);
    }
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(tableInfo, null, 2),
        },
      ],
    };
  }

  private async handleGetDatabaseStats(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    const stats = await db.getDatabaseStats();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(stats, null, 2),
        },
      ],
    };
  }

  private async handleValidateConnection(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    const isValid = await db.validateConnection();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ connected: isValid }, null, 2),
        },
      ],
    };
  }

  private async handleBeginTransaction(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    await db.beginTransaction();
    
    return {
      content: [
        {
          type: 'text',
          text: 'Transaction started',
        },
      ],
    };
  }

  private async handleCommitTransaction(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    await db.commitTransaction();
    
    return {
      content: [
        {
          type: 'text',
          text: 'Transaction committed',
        },
      ],
    };
  }

  private async handleRollbackTransaction(args: any): Promise<any> {
    const { connection } = args;
    const db = this.getConnection(connection);
    await db.rollbackTransaction();
    
    return {
      content: [
        {
          type: 'text',
          text: 'Transaction rolled back',
        },
      ],
    };
  }

  private async handleExecuteBatch(args: any): Promise<any> {
    const { operations, connection } = args;
    if (!operations || !Array.isArray(operations)) {
      throw new ValidationError('operations array is required');
    }

    const db = this.getConnection(connection);
    const results = await db.executeBatch(operations);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(results, null, 2),
        },
      ],
    };
  }

  private async handleAddConnection(args: any): Promise<any> {
    const { name, config } = args;
    if (!name || !config) {
      throw new ValidationError('name and config are required');
    }

    const validation = DatabaseAdapterFactory.validateConfig(config);
    if (!validation.valid) {
      throw new ValidationError(`Invalid configuration: ${validation.errors.join(', ')}`);
    }

    await this.connectionManager.addConnection(name, config);
    
    return {
      content: [
        {
          type: 'text',
          text: `Connection '${name}' added successfully`,
        },
      ],
    };
  }

  private async handleRemoveConnection(args: any): Promise<any> {
    const { name } = args;
    if (!name) {
      throw new ValidationError('name is required');
    }

    await this.connectionManager.removeConnection(name);
    
    return {
      content: [
        {
          type: 'text',
          text: `Connection '${name}' removed successfully`,
        },
      ],
    };
  }

  private async handleListConnections(): Promise<any> {
    const connections = this.connectionManager.listConnections();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(connections, null, 2),
        },
      ],
    };
  }

  private async handleSwitchConnection(args: any): Promise<any> {
    const { name } = args;
    if (!name) {
      throw new ValidationError('name is required');
    }

    this.connectionManager.setCurrentConnection(name);
    
    return {
      content: [
        {
          type: 'text',
          text: `Switched to connection '${name}'`,
        },
      ],
    };
  }

  private async handleGetCurrentConnection(): Promise<any> {
    const currentConnection = this.connectionManager.getCurrentConnectionName();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ currentConnection }, null, 2),
        },
      ],
    };
  }

  private async handleCompareSchemas(args: any): Promise<any> {
    const { source_connection, target_connection } = args;
    if (!source_connection || !target_connection) {
      throw new ValidationError('source_connection and target_connection are required');
    }

    const result = await this.schemaService.compareSchemas(source_connection, target_connection);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  private async handleGenerateMigration(args: any): Promise<any> {
    const { source_connection, target_connection, migration_name } = args;
    if (!source_connection || !target_connection || !migration_name) {
      throw new ValidationError('source_connection, target_connection, and migration_name are required');
    }

    const migration = await this.schemaService.generateMigration(source_connection, target_connection, migration_name);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(migration, null, 2),
        },
      ],
    };
  }

  private async handleGenerateDDL(args: any): Promise<any> {
    const { connection, include_data, include_indexes, include_constraints, format } = args;
    if (!connection) {
      throw new ValidationError('connection is required');
    }

    const options = {
      includeData: include_data || false,
      includeIndexes: include_indexes !== false,
      includeConstraints: include_constraints !== false,
      includeViews: true,
      includeFunctions: false,
      includeTriggers: false,
      format: (format || 'sql') as 'sql' | 'json' | 'yaml',
    };

    const ddl = await this.schemaService.generateDDL(connection, options);
    
    return {
      content: [
        {
          type: 'text',
          text: ddl,
        },
      ],
    };
  }

  private async handleValidateSchema(args: any): Promise<any> {
    const { connection } = args;
    if (!connection) {
      throw new ValidationError('connection is required');
    }

    const result = await this.schemaService.validateSchema(connection);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  private async handleExportData(args: any): Promise<any> {
    const { connection, output_path, format, tables, where_clause, limit, include_schema, pretty_print } = args;
    if (!connection || !output_path || !format) {
      throw new ValidationError('connection, output_path, and format are required');
    }

    const options = {
      format,
      tables,
      whereClause: where_clause,
      limit,
      includeSchema: include_schema !== false,
      prettyPrint: pretty_print || false,
    };

    const result = await this.exportImportService.exportData(connection, options, output_path);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  private async handleImportData(args: any): Promise<any> {
    const { connection, file_path, format, table_name, mode, batch_size, skip_errors, mapping } = args;
    if (!connection || !file_path || !format || !table_name) {
      throw new ValidationError('connection, file_path, format, and table_name are required');
    }

    const options = {
      format,
      tableName: table_name,
      mode: mode || 'insert',
      batchSize: batch_size || 1000,
      skipErrors: skip_errors || false,
      mapping,
    };

    const result = await this.exportImportService.importData(connection, file_path, options);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  private async handleAnalyzeQuery(args: any): Promise<any> {
    const { connection, query, parameters } = args;
    if (!connection || !query) {
      throw new ValidationError('connection and query are required');
    }

    const result = await this.queryAnalysisService.analyzeQuery(connection, query, parameters || []);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  private async handleGetQueryHistory(args: any): Promise<any> {
    const { limit } = args;
    const history = this.queryAnalysisService.getQueryHistory(limit || 50);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(history, null, 2),
        },
      ],
    };
  }

  private async handleGetSlowQueries(args: any): Promise<any> {
    const { threshold } = args;
    const slowQueries = this.queryAnalysisService.getSlowQueries(threshold || 1000);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(slowQueries, null, 2),
        },
      ],
    };
  }

  private async handleGetQueryStatistics(): Promise<any> {
    const stats = this.queryAnalysisService.getQueryStatistics();
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(stats, null, 2),
        },
      ],
    };
  }

  private async handleSaveQueryTemplate(args: any): Promise<any> {
    const { name, description, query, parameters, tags, connection_type } = args;
    if (!name || !description || !query || !parameters || !connection_type) {
      throw new ValidationError('name, description, query, parameters, and connection_type are required');
    }

    const template = await this.queryAnalysisService.saveTemplate({
      name,
      description,
      query,
      parameters,
      tags: tags || [],
      connectionType: connection_type,
    });
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(template, null, 2),
        },
      ],
    };
  }

  private async handleGetQueryTemplates(args: any): Promise<any> {
    const { connection_type, tags } = args;
    const templates = this.queryAnalysisService.getTemplates(connection_type, tags);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(templates, null, 2),
        },
      ],
    };
  }

  private async handleExecuteTemplate(args: any): Promise<any> {
    const { connection, template_id, parameters } = args;
    if (!connection || !template_id || !parameters) {
      throw new ValidationError('connection, template_id, and parameters are required');
    }

    const result = await this.queryAnalysisService.executeTemplate(connection, template_id, parameters);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  async start(): Promise<void> {
    if (this.config.databases && this.config.databases.length > 0) {
      for (const db of this.config.databases) {
        try {
          await this.connectionManager.addConnection(db.name, db);
        } catch (error) {
          console.error(`Failed to add connection ${db.name}:`, error);
        }
      }
    } else {
      console.error('No database connections configured');
    }

    const transport = new StdioServerTransport();
    await this.server.connect(transport);
  }

  async stop(): Promise<void> {
    await this.connectionManager.disconnectAll();
  }
}
