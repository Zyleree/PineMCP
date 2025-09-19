import { BaseDatabaseAdapter } from '../adapters/base-database-adapter.js';
import { DatabaseConnectionManager } from '../adapters/database-connection-manager.js';
import { QueryAnalysisResult, QueryTemplate } from '../types/schema.js';
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { ConnectionError, QueryError } from '../types/errors.js';

export class QueryAnalysisService {
  private connectionManager: DatabaseConnectionManager;
  private queryHistory: QueryAnalysisResult[] = [];
  private templates: QueryTemplate[] = [];
  private templatesPath: string;
  private readonly MAX_HISTORY_SIZE = 1000;

  constructor(connectionManager: DatabaseConnectionManager) {
    this.connectionManager = connectionManager;
    this.templatesPath = join(process.cwd(), 'data', 'query-templates.json');
    this.loadTemplates();
  }

  /**
   * Analyze query performance and provide recommendations
   */
  async analyzeQuery(
    connectionName: string,
    query: string,
    parameters: string[] = []
  ): Promise<QueryAnalysisResult> {
    const db = this.connectionManager.getConnection(connectionName);
    if (!db) {
      throw new ConnectionError('Connection not found', connectionName);
    }

    const startTime = Date.now();
    
    try {
      const result = await db.safeExecuteQuery(query, parameters);
      const executionTime = Date.now() - startTime;
      
      const performance = this.analyzePerformance(query, executionTime, result);
      
      let executionPlan;
      try {
        executionPlan = await this.getExecutionPlan(db, query);
      } catch (error) {
        executionPlan = null;
      }

      const analysis: QueryAnalysisResult = {
        query,
        executionTime,
        rowsAffected: result.rows?.length || 0,
        executionPlan,
        performance,
      };

      this.addToHistory(analysis);
      this.saveQueryHistory();

      return analysis;
    } catch (error) {
      const executionTime = Date.now() - startTime;
      return {
        query,
        executionTime,
        rowsAffected: 0,
        performance: {
          slow: true,
          score: 0,
          recommendations: [`Query failed: ${error}`],
        },
      };
    }
  }

  /**
   * Analyze query performance
   */
  private analyzePerformance(query: string, executionTime: number, _result: any): {
    slow: boolean;
    score: number;
    recommendations: string[];
  } {
    const recommendations: string[] = [];
    let score = 100;

    if (executionTime > 1000) {
      recommendations.push('Query is slow (>1s). Consider adding indexes or optimizing the query.');
      score -= 30;
    } else if (executionTime > 500) {
      recommendations.push('Query is moderately slow (>500ms). Consider optimization.');
      score -= 15;
    }

    const queryLower = query.toLowerCase();

    if (queryLower.includes('select *')) {
      recommendations.push('Avoid SELECT *. Specify only needed columns to improve performance.');
      score -= 10;
    }

    if (queryLower.includes('select') && !queryLower.includes('where') && !queryLower.includes('limit')) {
      recommendations.push('Consider adding WHERE clause or LIMIT to avoid full table scans.');
      score -= 20;
    }

    if (queryLower.includes("like '%")) {
      recommendations.push('LIKE with leading wildcard prevents index usage. Consider full-text search.');
      score -= 15;
    }

    if (queryLower.includes('order by') && !queryLower.includes('limit')) {
      recommendations.push('ORDER BY without LIMIT can be expensive. Consider adding LIMIT.');
      score -= 10;
    }

    if (queryLower.includes('select') && queryLower.includes('(select')) {
      recommendations.push('Consider converting subqueries to JOINs for better performance.');
      score -= 10;
    }

    const functionPatterns = [
      'where upper(',
      'where lower(',
      'where date(',
      'where year(',
      'where month(',
      'where day(',
    ];
    
    for (const pattern of functionPatterns) {
      if (queryLower.includes(pattern)) {
        recommendations.push('Functions in WHERE clause prevent index usage. Consider restructuring.');
        score -= 15;
        break;
      }
    }

    const orCount = (queryLower.match(/\bor\b/g) || []).length;
    if (orCount > 3) {
      recommendations.push('Multiple OR conditions can be slow. Consider using UNION or restructuring.');
      score -= 10;
    }

    const inMatch = queryLower.match(/in\s*\([^)]{100,}\)/);
    if (inMatch) {
      recommendations.push('Large IN clause can be slow. Consider using temporary table or EXISTS.');
      score -= 15;
    }

    if (queryLower.includes('join') && !queryLower.includes('on')) {
      recommendations.push('JOIN without ON clause. Ensure proper join conditions.');
      score -= 20;
    }

    score = Math.max(0, score);

    return {
      slow: executionTime > 1000 || score < 50,
      score,
      recommendations: recommendations.length > 0 ? recommendations : ['Query looks good!'],
    };
  }

  /**
   * Get execution plan for query (database-specific)
   */
  private async getExecutionPlan(db: BaseDatabaseAdapter, _query: string): Promise<any> {
    try {
      return {
        type: 'execution_plan',
        database: db.constructor.name,
        note: 'Execution plan not implemented for this database type',
      };
    } catch (error) {
      return null;
    }
  }

  /**
   * Add query to history with size limit management
   */
  private addToHistory(analysis: QueryAnalysisResult): void {
    this.queryHistory.push(analysis);
    
    if (this.queryHistory.length > this.MAX_HISTORY_SIZE) {
      this.queryHistory = this.queryHistory.slice(-this.MAX_HISTORY_SIZE);
    }
  }

  /**
   * Get query history
   */
  getQueryHistory(limit: number = 50): QueryAnalysisResult[] {
    return this.queryHistory.slice(-limit);
  }

  /**
   * Get slow queries
   */
  getSlowQueries(threshold: number = 1000): QueryAnalysisResult[] {
    return this.queryHistory.filter(q => q.executionTime > threshold);
  }

  /**
   * Get query statistics
   */
  getQueryStatistics(): {
    totalQueries: number;
    averageExecutionTime: number;
    slowQueries: number;
    mostCommonIssues: { issue: string; count: number }[];
  } {
    const totalQueries = this.queryHistory.length;
    const averageExecutionTime = totalQueries > 0 
      ? this.queryHistory.reduce((sum, q) => sum + q.executionTime, 0) / totalQueries 
      : 0;
    const slowQueries = this.queryHistory.filter(q => q.performance.slow).length;

    const issueCounts: { [key: string]: number } = {};
    this.queryHistory.forEach(q => {
      q.performance.recommendations.forEach(rec => {
        issueCounts[rec] = (issueCounts[rec] || 0) + 1;
      });
    });

    const mostCommonIssues = Object.entries(issueCounts)
      .map(([issue, count]) => ({ issue, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);

    return {
      totalQueries,
      averageExecutionTime: Math.round(averageExecutionTime),
      slowQueries,
      mostCommonIssues,
    };
  }

  /**
   * Save query template
   */
  async saveTemplate(template: Omit<QueryTemplate, 'id' | 'createdAt' | 'updatedAt'>): Promise<QueryTemplate> {
    const newTemplate: QueryTemplate = {
      ...template,
      id: `template_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    this.templates.push(newTemplate);
    this.saveTemplates();

    return newTemplate;
  }

  /**
   * Get query templates
   */
  getTemplates(connectionType?: string, tags?: string[]): QueryTemplate[] {
    let filtered = this.templates;

    if (connectionType) {
      filtered = filtered.filter(t => t.connectionType === connectionType);
    }

    if (tags && tags.length > 0) {
      filtered = filtered.filter(t => tags.some(tag => t.tags.includes(tag)));
    }

    return filtered;
  }

  /**
   * Get template by ID
   */
  getTemplate(id: string): QueryTemplate | null {
    return this.templates.find(t => t.id === id) || null;
  }

  /**
   * Update template
   */
  async updateTemplate(id: string, updates: Partial<QueryTemplate>): Promise<QueryTemplate | null> {
    const index = this.templates.findIndex(t => t.id === id);
    if (index === -1) return null;

    const existingTemplate = this.templates[index];
    if (!existingTemplate) return null;

    this.templates[index] = {
      name: existingTemplate.name,
      description: existingTemplate.description,
      query: existingTemplate.query,
      id: existingTemplate.id,
      parameters: existingTemplate.parameters,
      tags: existingTemplate.tags,
      connectionType: existingTemplate.connectionType,
      createdAt: existingTemplate.createdAt,
      ...updates,
      updatedAt: new Date().toISOString(),
    };

    this.saveTemplates();
    return this.templates[index] || null;
  }

  /**
   * Delete template
   */
  async deleteTemplate(id: string): Promise<boolean> {
    const index = this.templates.findIndex(t => t.id === id);
    if (index === -1) return false;

    this.templates.splice(index, 1);
    this.saveTemplates();
    return true;
  }

  /**
   * Execute template with parameters
   */
  async executeTemplate(
    connectionName: string,
    templateId: string,
    parameters: { [key: string]: any }
  ): Promise<QueryAnalysisResult> {
    const template = this.getTemplate(templateId);
    if (!template) {
      throw new QueryError('Template not found', undefined, templateId);
    }

    let query = template.query;
    for (const [key, value] of Object.entries(parameters)) {
      const placeholder = `{${key}}`;
      query = query.replace(new RegExp(placeholder, 'g'), String(value));
    }

    return this.analyzeQuery(connectionName, query);
  }

  /**
   * Load templates from file
   */
  private loadTemplates(): void {
    try {
      if (existsSync(this.templatesPath)) {
        const data = readFileSync(this.templatesPath, 'utf8');
        this.templates = JSON.parse(data);
      }
    } catch (error) {
      this.templates = [];
    }
  }

  /**
   * Save templates to file
   */
  private saveTemplates(): void {
    try {
      const dataDir = join(process.cwd(), 'data');
      if (!existsSync(dataDir)) {
        mkdirSync(dataDir, { recursive: true });
      }
      writeFileSync(this.templatesPath, JSON.stringify(this.templates, null, 2));
    } catch (error) {
    }
  }

  /**
   * Save query history to file
   */
  private saveQueryHistory(): void {
    try {
      const historyPath = join(process.cwd(), 'data', 'query-history.json');
      const dataDir = join(process.cwd(), 'data');
      if (!existsSync(dataDir)) {
        mkdirSync(dataDir, { recursive: true });
      }
      writeFileSync(historyPath, JSON.stringify(this.queryHistory, null, 2));
    } catch (error) {
    }
  }

  /**
   * Clear query history
   */
  clearHistory(): void {
    this.queryHistory = [];
    this.saveQueryHistory();
  }

  /**
   * Get memory usage statistics
   */
  getMemoryStats(): { historySize: number; maxSize: number; memoryUsage: string } {
    const historySize = this.queryHistory.length;
    const maxSize = this.MAX_HISTORY_SIZE;
    
    const avgQuerySize = 500; // bytes per query (rough estimate)
    const memoryUsageBytes = historySize * avgQuerySize;
    const memoryUsageMB = (memoryUsageBytes / 1024 / 1024).toFixed(2);
    
    return {
      historySize,
      maxSize,
      memoryUsage: `${memoryUsageMB} MB`
    };
  }

  /**
   * Export query history
   */
  exportHistory(format: 'json' | 'csv' = 'json'): string {
    if (format === 'csv') {
      const headers = ['query', 'executionTime', 'rowsAffected', 'slow', 'score'];
      const rows = this.queryHistory.map(q => [
        q.query.replace(/\n/g, ' '),
        q.executionTime,
        q.rowsAffected,
        q.performance.slow,
        q.performance.score,
      ]);
      
      return [headers.join(','), ...rows.map(row => row.join(','))].join('\n');
    }
    
    return JSON.stringify(this.queryHistory, null, 2);
  }
}
