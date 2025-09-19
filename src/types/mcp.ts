import { z } from 'zod';

export const MCPConfig = z.object({
  databases: z.array(z.object({
    name: z.string(),
    type: z.enum(['postgresql', 'mysql', 'sqlite', 'redis', 'mongodb', 'cassandra', 'mssql', 'dynamodb']),
    url: z.string().optional(),
    host: z.string().optional(),
    port: z.number().optional(),
    database: z.string().optional(),
    username: z.string().optional(),
    password: z.string().optional(),
    ssl: z.boolean().optional(),
    filename: z.string().optional(),
    db: z.number().optional(),
    authSource: z.string().optional(),
    keyspace: z.string().optional(),
    datacenter: z.string().optional(),
  })),
  server: z.object({
    name: z.string().default('PineMCP'),
    version: z.string().default('2.1.0'),
    description: z.string().default('A professional MCP server supporting multiple database types'),
  }),
  logging: z.object({
    level: z.enum(['error', 'warn', 'info', 'debug']).default('info'),
    format: z.enum(['json', 'text']).default('text'),
  }),
});

export type MCPConfig = z.infer<typeof MCPConfig>;

export interface MCPTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export interface MCPResource {
  uri: string;
  name: string;
  description?: string;
  mimeType?: string;
}

export interface MCPPrompt {
  name: string;
  description: string;
  arguments?: Array<{
    name: string;
    description: string;
    required: boolean;
  }>;
}
