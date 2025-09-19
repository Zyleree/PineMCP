export class DatabaseError extends Error {
  public readonly isOperational: boolean = true;
  public readonly databaseType?: string;
  public readonly operation?: string;

  constructor(message: string, databaseType?: string, operation?: string) {
    super(message);
    this.name = 'DatabaseError';
    if (databaseType !== undefined) {
      this.databaseType = databaseType;
    }
    if (operation !== undefined) {
      this.operation = operation;
    }
  }
}

export class ConnectionError extends DatabaseError {
  constructor(message: string, databaseType?: string) {
    super(message, databaseType, 'connection');
    this.name = 'ConnectionError';
  }
}

export class TransactionError extends DatabaseError {
  constructor(message: string, databaseType?: string) {
    super(message, databaseType, 'transaction');
    this.name = 'TransactionError';
  }
}

export class QueryError extends DatabaseError {
  public readonly query?: string;
  public readonly parameters?: unknown[];

  constructor(message: string, databaseType?: string, query?: string, parameters?: unknown[]) {
    super(message, databaseType, 'query');
    this.name = 'QueryError';
    if (query !== undefined) {
      this.query = query;
    }
    if (parameters !== undefined) {
      this.parameters = parameters;
    }
  }
}

export class ValidationError extends Error {
  public readonly field?: string;
  public readonly value?: unknown;

  constructor(message: string, field?: string, value?: unknown) {
    super(message);
    this.name = 'ValidationError';
    if (field !== undefined) {
      this.field = field;
    }
    if (value !== undefined) {
      this.value = value;
    }
  }
}

export class ConfigurationError extends Error {
  public readonly configPath?: string;

  constructor(message: string, configPath?: string) {
    super(message);
    this.name = 'ConfigurationError';
    if (configPath !== undefined) {
      this.configPath = configPath;
    }
  }
}
