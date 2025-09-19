import { DatabaseConnectionManager } from '../adapters/database-connection-manager.js';
import { 
  SchemaComparisonResult, 
  Migration, 
  MigrationStep, 
  DDLGenerationOptions 
} from '../types/schema.js';
import { TableInfo, ColumnInfo } from '../types/database.js';
import { ConnectionError, QueryError } from '../types/errors.js';

interface SchemaDifference {
  type: 'table_added' | 'table_removed' | 'table_modified' | 'column_added' | 'column_removed' | 'column_modified' | 'index_added' | 'index_removed' | 'index_modified' | 'constraint_added' | 'constraint_removed' | 'constraint_modified';
  tableName: string;
  columnName?: string;
  details: string;
  sourceValue?: any;
  targetValue?: any;
}

export class SchemaManagementService {
  private connectionManager: DatabaseConnectionManager;

  constructor(connectionManager: DatabaseConnectionManager) {
    this.connectionManager = connectionManager;
  }

  /**
   * Compare schemas between two database connections
   */
  async compareSchemas(
    sourceConnection: string,
    targetConnection: string
  ): Promise<SchemaComparisonResult> {
    const sourceDb = this.connectionManager.getConnection(sourceConnection);
    const targetDb = this.connectionManager.getConnection(targetConnection);

    if (!sourceDb || !targetDb) {
      throw new ConnectionError('One or both connections not found');
    }

    const sourceTables = await sourceDb.getTables();
    const targetTables = await targetDb.getTables();

    const differences: any[] = [];
    let tablesAdded = 0;
    let tablesRemoved = 0;
    let tablesModified = 0;
    let columnsAdded = 0;
    let columnsRemoved = 0;
    let columnsModified = 0;
    
    for (const sourceTable of sourceTables) {
      const targetTable = targetTables.find(t => t.name === sourceTable.name);
      if (!targetTable) {
        differences.push({
          type: 'table_added',
          tableName: sourceTable.name,
          details: `Table '${sourceTable.name}' exists in source but not in target`,
        });
        tablesAdded++;
      } else {
        try {
          const sourceInfo = await sourceDb.getTableInfo(sourceTable.name);
          const targetInfo = await targetDb.getTableInfo(targetTable.name);
          
          if (!sourceInfo) {
            differences.push({
              type: 'table_modified',
              tableName: sourceTable.name,
              details: `Table '${sourceTable.name}' exists in source but cannot be accessed`,
            });
            tablesModified++;
            continue;
          }
          
          if (!targetInfo) {
            differences.push({
              type: 'table_modified',
              tableName: sourceTable.name,
              details: `Table '${sourceTable.name}' exists in target but cannot be accessed`,
            });
            tablesModified++;
            continue;
          }
          
          const tableDiffs = this.compareTableStructures(sourceTable.name, sourceInfo, targetInfo);
          differences.push(...tableDiffs);
          
          if (tableDiffs.length > 0) {
            tablesModified++;
          }
        } catch (error) {
          differences.push({
            type: 'table_modified',
            tableName: sourceTable.name,
            details: `Error comparing table '${sourceTable.name}': ${error instanceof Error ? error.message : 'Unknown error'}`,
          });
          tablesModified++;
        }
      }
    }

    for (const targetTable of targetTables) {
      const sourceTable = sourceTables.find(t => t.name === targetTable.name);
      if (!sourceTable) {
        differences.push({
          type: 'table_removed',
          tableName: targetTable.name,
          details: `Table '${targetTable.name}' exists in target but not in source`,
        });
        tablesRemoved++;
      }
    }

    for (const diff of differences) {
      if (diff.type === 'column_added') columnsAdded++;
      else if (diff.type === 'column_removed') columnsRemoved++;
      else if (diff.type === 'column_modified') columnsModified++;
    }

    return {
      identical: differences.length === 0,
      differences,
      summary: {
        tablesAdded,
        tablesRemoved,
        tablesModified,
        columnsAdded,
        columnsRemoved,
        columnsModified,
      },
    };
  }

  /**
   * Compare table structures between source and target
   */
  private compareTableStructures(
    tableName: string,
    sourceInfo: TableInfo | null,
    targetInfo: TableInfo | null
  ): SchemaDifference[] {
    const differences: SchemaDifference[] = [];
    
    if (!sourceInfo || !targetInfo) {
      return differences;
    }
    
    const sourceColumns = sourceInfo.columns || [];
    const targetColumns = targetInfo.columns || [];

    for (const sourceCol of sourceColumns) {
      const targetCol = targetColumns.find((c: any) => c.name === sourceCol.name);
      if (!targetCol) {
        differences.push({
          type: 'column_added',
          tableName,
          columnName: sourceCol.name,
          details: `Column '${sourceCol.name}' exists in source but not in target`,
          sourceValue: sourceCol,
        });
      } else {
        const colDiffs = this.compareColumnProperties(sourceCol, targetCol);
        if (colDiffs.length > 0) {
          differences.push({
            type: 'column_modified',
            tableName,
            columnName: sourceCol.name,
            details: colDiffs.join(', '),
            sourceValue: sourceCol,
            targetValue: targetCol,
          });
        }
      }
    }

    for (const targetCol of targetColumns) {
      const sourceCol = sourceColumns.find((c: any) => c.name === targetCol.name);
      if (!sourceCol) {
        differences.push({
          type: 'column_removed',
          tableName,
          columnName: targetCol.name,
          details: `Column '${targetCol.name}' exists in target but not in source`,
          targetValue: targetCol,
        });
      }
    }

    const sourceIndexes = sourceInfo.indexes || [];
    const targetIndexes = targetInfo.indexes || [];

    for (const sourceIdx of sourceIndexes) {
      const targetIdx = targetIndexes.find((i: any) => i.name === sourceIdx.name);
      if (!targetIdx) {
        differences.push({
          type: 'index_added',
          tableName,
          details: `Index '${sourceIdx.name}' exists in source but not in target`,
          sourceValue: sourceIdx,
        });
      }
    }

    for (const targetIdx of targetIndexes) {
      const sourceIdx = sourceIndexes.find((i: any) => i.name === targetIdx.name);
      if (!sourceIdx) {
        differences.push({
          type: 'index_removed',
          tableName,
          details: `Index '${targetIdx.name}' exists in target but not in source`,
          targetValue: targetIdx,
        });
      }
    }

    return differences;
  }

  /**
   * Compare column properties
   */
  private compareColumnProperties(sourceCol: ColumnInfo, targetCol: ColumnInfo): string[] {
    const differences: string[] = [];

    if (!sourceCol || !targetCol) {
      return differences;
    }

    const sourceType = sourceCol.dataType;
    const targetType = targetCol.dataType;
    if (sourceType !== targetType) {
      differences.push(`type: ${sourceType} vs ${targetType}`);
    }

    if (sourceCol.nullable !== targetCol.nullable) {
      differences.push(`nullable: ${sourceCol.nullable} vs ${targetCol.nullable}`);
    }
    if (sourceCol.defaultValue !== targetCol.defaultValue) {
      differences.push(`default: ${sourceCol.defaultValue} vs ${targetCol.defaultValue}`);
    }
    if (sourceCol.maxLength !== targetCol.maxLength) {
      differences.push(`maxLength: ${sourceCol.maxLength} vs ${targetCol.maxLength}`);
    }

    return differences;
  }

  /**
   * Generate migration from schema comparison
   */
  async generateMigration(
    sourceConnection: string,
    targetConnection: string,
    migrationName: string
  ): Promise<Migration> {
    const comparison = await this.compareSchemas(sourceConnection, targetConnection);
    
    if (comparison.identical) {
      throw new QueryError('Schemas are identical, no migration needed');
    }

    const steps: MigrationStep[] = [];
    const migrationId = `migration_${Date.now()}`;

    for (const diff of comparison.differences) {
      const step = await this.createMigrationStep(diff, sourceConnection);
      if (step) {
        steps.push(step);
      }
    }

    return {
      id: migrationId,
      name: migrationName,
      description: `Migration from ${sourceConnection} to ${targetConnection}`,
      timestamp: new Date().toISOString(),
      steps,
      sourceConnection,
      targetConnection,
    };
  }

  /**
   * Create migration step from difference
   */
  private async createMigrationStep(diff: any, _sourceConnection: string): Promise<MigrationStep | null> {
    const stepId = `step_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    switch (diff.type) {
      case 'table_added':
        return {
          id: stepId,
          type: 'create_table',
          tableName: diff.tableName,
          sql: `CREATE TABLE ${diff.tableName} (...);`,
          rollbackSql: `DROP TABLE ${diff.tableName};`,
          description: `Create table ${diff.tableName}`,
        };

      case 'table_removed':
        return {
          id: stepId,
          type: 'drop_table',
          tableName: diff.tableName,
          sql: `DROP TABLE ${diff.tableName};`,
          rollbackSql: `-- Rollback requires table recreation`,
          description: `Drop table ${diff.tableName}`,
        };

      case 'column_added':
        return {
          id: stepId,
          type: 'add_column',
          tableName: diff.tableName,
          columnName: diff.columnName,
          sql: `ALTER TABLE ${diff.tableName} ADD COLUMN ${diff.columnName} ...;`,
          rollbackSql: `ALTER TABLE ${diff.tableName} DROP COLUMN ${diff.columnName};`,
          description: `Add column ${diff.columnName} to ${diff.tableName}`,
        };

      case 'column_removed':
        return {
          id: stepId,
          type: 'drop_column',
          tableName: diff.tableName,
          columnName: diff.columnName,
          sql: `ALTER TABLE ${diff.tableName} DROP COLUMN ${diff.columnName};`,
          rollbackSql: `-- Rollback requires column recreation`,
          description: `Drop column ${diff.columnName} from ${diff.tableName}`,
        };

      case 'column_modified':
        return {
          id: stepId,
          type: 'modify_column',
          tableName: diff.tableName,
          columnName: diff.columnName,
          sql: `ALTER TABLE ${diff.tableName} MODIFY COLUMN ${diff.columnName} ...;`,
          rollbackSql: `-- Rollback requires column modification`,
          description: `Modify column ${diff.columnName} in ${diff.tableName}`,
        };

      case 'index_added':
        return {
          id: stepId,
          type: 'add_index',
          tableName: diff.tableName,
          sql: `CREATE INDEX ${diff.sourceValue?.name} ON ${diff.tableName} (...);`,
          rollbackSql: `DROP INDEX ${diff.sourceValue?.name};`,
          description: `Add index ${diff.sourceValue?.name} to ${diff.tableName}`,
        };

      case 'index_removed':
        return {
          id: stepId,
          type: 'drop_index',
          tableName: diff.tableName,
          sql: `DROP INDEX ${diff.targetValue?.name};`,
          rollbackSql: `-- Rollback requires index recreation`,
          description: `Drop index ${diff.targetValue?.name} from ${diff.tableName}`,
        };

      default:
        return null;
    }
  }

  /**
   * Generate DDL for a database connection
   */
  async generateDDL(
    connectionName: string,
    options: DDLGenerationOptions = {
      includeData: false,
      includeIndexes: true,
      includeConstraints: true,
      includeViews: true,
      includeFunctions: false,
      includeTriggers: false,
      format: 'sql',
    }
  ): Promise<string> {
    const db = this.connectionManager.getConnection(connectionName);
    if (!db) {
      throw new ConnectionError('Connection not found', connectionName);
    }

    const tables = await db.getTables();
    const ddlStatements: string[] = [];
    const ddlObjects: any[] = [];

    for (const table of tables) {
      const tableInfo = await db.getTableInfo(table.name);
      if (tableInfo) {
        const tableDDL = this.generateTableDDL(table.name, tableInfo, options);
        ddlStatements.push(tableDDL);
        ddlObjects.push({ table: table.name, ddl: tableDDL, info: tableInfo });
      }
    }

    if (options.format === 'json') {
      return JSON.stringify({ connection: connectionName, tables: ddlObjects }, null, 2);
    }
    if (options.format === 'yaml') {
      const toYaml = (obj: any, indent: string = ''): string => {
        if (Array.isArray(obj)) {
          return obj.map(item => `${indent}- ${typeof item === 'object' ? '\n' + toYaml(item, indent + '  ') : String(item)}`).join('\n');
        }
        if (obj && typeof obj === 'object') {
          return Object.entries(obj).map(([k, v]) => {
            if (v && typeof v === 'object') {
              return `${indent}${k}:\n${toYaml(v, indent + '  ')}`;
            }
            return `${indent}${k}: ${String(v)}`;
          }).join('\n');
        }
        return `${indent}${String(obj)}`;
      };
      return toYaml({ connection: connectionName, tables: ddlObjects });
    }
    return ddlStatements.join('\n\n');
  }

  /**
   * Generate DDL for a specific table
   */
  private generateTableDDL(tableName: string, tableInfo: any, options: DDLGenerationOptions): string {
    const columns = tableInfo.columns || [];
    const indexes = tableInfo.indexes || [];
    const constraints = tableInfo.constraints || [];

    let ddl = `CREATE TABLE ${tableName} (\n`;
    
    const columnDefs = columns.map((col: any) => {
      let def = `  ${col.name} ${col.dataType || col.type || 'VARCHAR'}`;
      if (col.maxLength) def += `(${col.maxLength})`;
      if (!col.nullable) def += ' NOT NULL';
      if (col.defaultValue) def += ` DEFAULT ${col.defaultValue}`;
      return def;
    });
    
    ddl += columnDefs.join(',\n');
    
    if (constraints.length > 0) {
      ddl += ',\n';
      const constraintDefs = constraints.map((constraint: any) => {
        return `  ${constraint.name} ${constraint.type} (${constraint.columns?.join(', ') || ''})`;
      });
      ddl += constraintDefs.join(',\n');
    }
    
    ddl += '\n);';
    
    if (options.includeIndexes && indexes.length > 0) {
      ddl += '\n\n';
      const indexDefs = indexes.map((index: any) => {
        return `CREATE INDEX ${index.name} ON ${tableName} (${index.columns?.join(', ') || ''});`;
      });
      ddl += indexDefs.join('\n');
    }

    return ddl;
  }

  /**
   * Validate schema consistency
   */
  async validateSchema(connectionName: string): Promise<{ valid: boolean; issues: string[] }> {
    const db = this.connectionManager.getConnection(connectionName);
    if (!db) {
      throw new ConnectionError('Connection not found', connectionName);
    }

    const issues: string[] = [];
    const tables = await db.getTables();

    for (const table of tables) {
      try {
        const tableInfo = await db.getTableInfo(table.name);
        if (!tableInfo) {
          issues.push(`Table '${table.name}' not found or inaccessible`);
          continue;
        }
        
        const hasPrimaryKey = (tableInfo.constraints || []).some((c: any) => c.type === 'PRIMARY KEY');
        if (!hasPrimaryKey) {
          issues.push(`Table '${table.name}' has no primary key`);
        }

        const columns = tableInfo.columns || [];
        for (const col of columns) {
          if (!col.dataType && !('type' in col)) {
            issues.push(`Column '${col.name}' in table '${table.name}' has no type`);
          }
        }
      } catch (error) {
        issues.push(`Error validating table '${table.name}': ${error}`);
      }
    }

    return {
      valid: issues.length === 0,
      issues,
    };
  }
}
