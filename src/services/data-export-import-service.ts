import { BaseDatabaseAdapter } from '../adapters/base-database-adapter.js';
import { DatabaseConnectionManager } from '../adapters/database-connection-manager.js';
import { ExportOptions, ImportOptions } from '../types/schema.js';
import { writeFileSync, readFileSync } from 'fs';
import { ConnectionError } from '../types/errors.js';

export class DataExportImportService {
  private connectionManager: DatabaseConnectionManager;

  constructor(connectionManager: DatabaseConnectionManager) {
    this.connectionManager = connectionManager;
  }

  /**
   * Export data from database to file
   */
  async exportData(
    connectionName: string,
    options: ExportOptions,
    outputPath: string
  ): Promise<{ success: boolean; message: string; recordCount: number }> {
    const db = this.connectionManager.getConnection(connectionName);
    if (!db) {
      throw new ConnectionError('Connection not found', connectionName);
    }

    const sqlLike = ['postgresql', 'mysql', 'sqlite', 'mssql'];
    if (!sqlLike.includes(db.getType())) {
      return { success: false, message: `Export not supported for ${db.getType()}`, recordCount: 0 };
    }

    try {
      const data: any[] = [];
      let recordCount = 0;

      if (options.tables && options.tables.length > 0) {
        for (const tableName of options.tables) {
          const tableData = await this.exportTableData(db, tableName, options);
          data.push({
            table: tableName,
            data: tableData,
            recordCount: tableData.length,
          });
          recordCount += tableData.length;
        }
      } else {
        const tables = await db.getTables();
        for (const table of tables) {
          const tableData = await this.exportTableData(db, table.name, options);
          data.push({
            table: table.name,
            data: tableData,
            recordCount: tableData.length,
          });
          recordCount += tableData.length;
        }
      }

      let output: string;
      switch (options.format) {
        case 'json':
          output = this.generateJSONOutput(data, options);
          break;
        case 'csv':
          output = this.generateCSVOutput(data, options);
          break;
        case 'sql':
          output = this.generateSQLOutput(data, options);
          break;
        case 'xml':
          output = this.generateXMLOutput(data, options);
          break;
        default:
          throw new Error(`Unsupported export format: ${options.format}`);
      }

      writeFileSync(outputPath, output, 'utf8');

      return {
        success: true,
        message: `Data exported successfully to ${outputPath}`,
        recordCount,
      };
    } catch (error) {
      return {
        success: false,
        message: `Export failed: ${error}`,
        recordCount: 0,
      };
    }
  }

  /**
   * Export data from a specific table
   */
  private async exportTableData(
    db: BaseDatabaseAdapter,
    tableName: string,
    options: ExportOptions
  ): Promise<any[]> {
    let query = `SELECT * FROM ${tableName}`;
    
    if (options.whereClause) {
      query += ` WHERE ${options.whereClause}`;
    }
    
    if (options.limit) {
      query += ` LIMIT ${options.limit}`;
    }

    const result = await db.executeQuery(query);
    return result.rows || [];
  }

  /**
   * Generate JSON output
   */
  private generateJSONOutput(data: any[], options: ExportOptions): string {
    const output = {
      exportedAt: new Date().toISOString(),
      format: 'json',
      tables: data,
    };

    return options.prettyPrint 
      ? JSON.stringify(output, null, 2)
      : JSON.stringify(output);
  }

  /**
   * Generate CSV output
   */
  private generateCSVOutput(data: any[], _options: ExportOptions): string {
    let csv = '';
    
    for (const tableData of data) {
      if (tableData.data.length === 0) continue;
      
      csv += `# Table: ${tableData.table}\n`;
      
      const headers = Object.keys(tableData.data[0]);
      csv += headers.join(',') + '\n';
      
      for (const row of tableData.data) {
        const values = headers.map(header => {
          const value = row[header];
          if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
            return `"${value.replace(/"/g, '""')}"`;
          }
          return value || '';
        });
        csv += values.join(',') + '\n';
      }
      
      csv += '\n';
    }
    
    return csv;
  }

  /**
   * Generate SQL output
   */
  private generateSQLOutput(data: any[], _options: ExportOptions): string {
    let sql = `-- Data export generated at ${new Date().toISOString()}\n\n`;
    
    for (const tableData of data) {
      if (tableData.data.length === 0) continue;
      
      sql += `-- Table: ${tableData.table}\n`;
      
      for (const row of tableData.data) {
        const columns = Object.keys(row);
        const values = Object.values(row).map(value => {
          if (value === null) return 'NULL';
          if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
          return value;
        });
        
        sql += `INSERT INTO ${tableData.table} (${columns.join(', ')}) VALUES (${values.join(', ')});\n`;
      }
      
      sql += '\n';
    }
    
    return sql;
  }

  /**
   * Generate XML output
   */
  private generateXMLOutput(data: any[], _options: ExportOptions): string {
    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
    xml += `<export generatedAt="${new Date().toISOString()}" format="xml">\n`;
    
    for (const tableData of data) {
      if (tableData.data.length === 0) continue;
      
      xml += `  <table name="${tableData.table}">\n`;
      
      for (const row of tableData.data) {
        xml += '    <record>\n';
        for (const [key, value] of Object.entries(row)) {
          const escapedValue = String(value || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          xml += `      <${key}>${escapedValue}</${key}>\n`;
        }
        xml += '    </record>\n';
      }
      
      xml += '  </table>\n';
    }
    
    xml += '</export>';
    return xml;
  }

  /**
   * Import data from file to database
   */
  async importData(
    connectionName: string,
    filePath: string,
    options: ImportOptions
  ): Promise<{ success: boolean; message: string; recordCount: number }> {
    const db = this.connectionManager.getConnection(connectionName);
    if (!db) {
      throw new ConnectionError('Connection not found', connectionName);
    }

    try {
      const fileContent = readFileSync(filePath, 'utf8');
      const data = this.parseImportData(fileContent, options.format);
      
      let recordCount = 0;
      const batchSize = options.batchSize || 1000;
      
      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        await this.importBatch(db, options.tableName, batch, options);
        recordCount += batch.length;
      }

      return {
        success: true,
        message: `Data imported successfully to ${options.tableName}`,
        recordCount,
      };
    } catch (error) {
      return {
        success: false,
        message: `Import failed: ${error}`,
        recordCount: 0,
      };
    }
  }

  /**
   * Parse import data based on format
   */
  private parseImportData(content: string, format: string): any[] {
    switch (format) {
      case 'json': {
        const jsonData = JSON.parse(content);
        if (jsonData.tables) {
          return jsonData.tables.flatMap((table: any) => table.data || []);
        }
        return Array.isArray(jsonData) ? jsonData : [jsonData];
      }
        
      case 'csv':
        return this.parseCSVData(content);
        
      case 'sql':
        return this.parseSQLData(content);
        
      case 'xml':
        return this.parseXMLData(content);
        
      default:
        throw new Error(`Unsupported import format: ${format}`);
    }
  }

  /**
   * Parse CSV data
   */
  private parseCSVData(content: string): any[] {
    const lines = content.split('\n').filter(line => line.trim() && !line.startsWith('#'));
    if (lines.length === 0) return [];
    
    const firstLine = lines[0];
    if (!firstLine) return [];
    
    const headers = firstLine.split(',').map(h => h.trim());
    const data: any[] = [];
    
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i];
      if (!line) continue;
      
      const values = this.parseCSVLine(line);
      if (values.length === headers.length) {
        const row: any = {};
        headers.forEach((header, index) => {
          row[header] = values[index];
        });
        data.push(row);
      }
    }
    
    return data;
  }

  /**
   * Parse a single CSV line handling quoted values
   */
  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    
    values.push(current.trim());
    return values;
  }

  /**
   * Parse SQL data (extract INSERT statements)
   */
  private parseSQLData(content: string): any[] {
    const data: any[] = [];
    const insertRegex = /INSERT INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/gi;
    let match;
    
    while ((match = insertRegex.exec(content)) !== null) {
      const tableName = match[1];
      const columnsStr = match[2];
      const valuesStr = match[3];
      
      if (!tableName || !columnsStr || !valuesStr) continue;
      
      const columns = columnsStr.split(',').map(c => c.trim());
      const values = this.parseSQLValues(valuesStr);
      
      if (columns.length === values.length) {
        const row: any = {};
        columns.forEach((column, index) => {
          row[column] = values[index];
        });
        data.push(row);
      }
    }
    
    return data;
  }

  /**
   * Parse SQL VALUES clause
   */
  private parseSQLValues(valuesStr: string): any[] {
    const values: any[] = [];
    let current = '';
    let inQuotes = false;
    let quoteChar = '';
    
    for (let i = 0; i < valuesStr.length; i++) {
      const char = valuesStr[i];
      
      if ((char === "'" || char === '"') && !inQuotes) {
        inQuotes = true;
        quoteChar = char;
      } else if (char === quoteChar && inQuotes) {
        if (valuesStr[i + 1] === quoteChar) {
          current += char;
          i++;
        } else {
          inQuotes = false;
          quoteChar = '';
        }
      } else if (char === ',' && !inQuotes) {
        values.push(this.parseSQLValue(current.trim()));
        current = '';
      } else {
        current += char;
      }
    }
    
    values.push(this.parseSQLValue(current.trim()));
    return values;
  }

  /**
   * Parse a single SQL value
   */
  private parseSQLValue(value: string): any {
    if (value === 'NULL') return null;
    if (value.startsWith("'") && value.endsWith("'")) {
      return value.slice(1, -1).replace(/''/g, "'");
    }
    if (value.startsWith('"') && value.endsWith('"')) {
      return value.slice(1, -1).replace(/""/g, '"');
    }
    if (!isNaN(Number(value))) return Number(value);
    return value;
  }

  /**
   * Parse XML data
   */
  private parseXMLData(content: string): any[] {
    const data: any[] = [];
    const recordRegex = /<record>(.*?)<\/record>/gs;
    let match;
    
    while ((match = recordRegex.exec(content)) !== null) {
      const recordContent = match[1];
      if (!recordContent) continue;
      
      const fieldRegex = /<(\w+)>(.*?)<\/\1>/g;
      const row: any = {};
      let fieldMatch;
      
      while ((fieldMatch = fieldRegex.exec(recordContent)) !== null) {
        const fieldName = fieldMatch[1];
        const fieldValue = fieldMatch[2];
        
        if (!fieldName || fieldValue === undefined) continue;
        
        const processedValue = fieldValue
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>');
        row[fieldName] = processedValue;
      }
      
      data.push(row);
    }
    
    return data;
  }

  /**
   * Import a batch of data
   */
  private async importBatch(
    db: BaseDatabaseAdapter,
    tableName: string,
    batch: any[],
    options: ImportOptions
  ): Promise<void> {
    if (batch.length === 0) return;
    
    const sqlLike = ['postgresql', 'mysql', 'sqlite', 'mssql'];
    if (!sqlLike.includes(db.getType())) {
      throw new Error(`Import not supported for ${db.getType()}`);
    }

    const columns = Object.keys(batch[0]);
    const placeholders = columns.map(() => '?').join(', ');
    
    for (const row of batch) {
      try {
        const values = columns.map(col => {
          const mappedCol = options.mapping?.[col] || col;
          return row[mappedCol];
        });
        
        let query: string;
        switch (options.mode) {
          case 'insert':
            query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
            break;
          case 'upsert':
            if (db.getType() === 'postgresql') {
              query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO NOTHING`;
            } else if (db.getType() === 'mysql') {
              const updates = columns.map(c => `${c}=VALUES(${c})`).join(', ');
              query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders}) ON DUPLICATE KEY UPDATE ${updates}`;
            } else if (db.getType() === 'mssql') {
              query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
            } else if (db.getType() === 'sqlite') {
              const updates = columns.map(c => `${c}=excluded.${c}`).join(', ');
              query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO UPDATE SET ${updates}`;
            } else {
              query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
            }
            break;
          case 'replace':
            query = `REPLACE INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
            break;
          default:
            query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
        }
        
        await db.executeQuery(query, values);
      } catch (error) {
        if (!options.skipErrors) {
          throw error;
        }
      }
    }
  }
}
