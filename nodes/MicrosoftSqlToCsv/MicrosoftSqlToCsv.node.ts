import { once } from 'node:events';
import { createWriteStream, type WriteStream } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import sql from 'mssql';
import { tmpdir } from 'node:os';
import path from 'node:path';

import type {
	ICredentialDataDecryptedObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import {
	ApplicationError,
	NodeConnectionTypes,
	NodeOperationError,
	sanitizeFilename,
} from 'n8n-workflow';

const DEFAULT_BINARY_PROPERTY = 'data';
const DEFAULT_DELIMITER = ',';
const DEFAULT_FILE_NAME = 'query-results.csv';
const CSV_MIME_TYPE = 'text/csv';
const LINE_BREAK = '\n';
const QUOTE_CHARACTER = '"';

type MicrosoftSqlCredentialData = ICredentialDataDecryptedObject;

interface SqlColumnMetadata {
	name: string;
}

interface SqlConnectionConfig {
	server: string;
	port: number;
	database: string;
	user: string;
	password: string;
	connectionTimeout: number;
	requestTimeout: number;
	options: {
		enableArithAbort: boolean;
		encrypt: boolean;
		trustServerCertificate: boolean;
		instanceName?: string;
	};
}

interface CsvFileResult {
	columnCount: number;
	rowCount: number;
	tempFilePath: string;
}

function toOptionalString(value: unknown): string | undefined {
	if (typeof value !== 'string') {
		return undefined;
	}

	const trimmedValue = value.trim();

	return trimmedValue === '' ? undefined : trimmedValue;
}

function toRequiredString(value: unknown, fieldName: string): string {
	const stringValue = toOptionalString(value);

	if (stringValue === undefined) {
		throw new ApplicationError(`${fieldName} is required.`);
	}

	return stringValue;
}

function toNumber(value: unknown, fallbackValue: number): number {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return value;
	}

	if (typeof value === 'string' && value.trim() !== '') {
		const parsedValue = Number(value);

		if (Number.isFinite(parsedValue)) {
			return parsedValue;
		}
	}

	return fallbackValue;
}

function toBoolean(value: unknown, fallbackValue: boolean): boolean {
	if (typeof value === 'boolean') {
		return value;
	}

	if (typeof value === 'string') {
		if (value === 'true') {
			return true;
		}

		if (value === 'false') {
			return false;
		}
	}

	return fallbackValue;
}

function normalizeFileName(fileName: string): string {
	const baseFileName = path.basename(fileName);
	const sanitizedFileName = sanitizeFilename(baseFileName) || DEFAULT_FILE_NAME;

	return sanitizedFileName.toLowerCase().endsWith('.csv')
		? sanitizedFileName
		: `${sanitizedFileName}.csv`;
}

function escapeCsvValue(value: unknown, delimiter: string): string {
	if (value === null || value === undefined) {
		return '';
	}

	let normalizedValue: string;

	if (value instanceof Date) {
		normalizedValue = value.toISOString();
	} else if (Buffer.isBuffer(value)) {
		normalizedValue = value.toString('base64');
	} else if (typeof value === 'object') {
		normalizedValue = JSON.stringify(value);
	} else {
		normalizedValue = String(value);
	}

	const needsQuoting =
		normalizedValue.includes(delimiter) ||
		normalizedValue.includes(QUOTE_CHARACTER) ||
		normalizedValue.includes('\r') ||
		normalizedValue.includes('\n');

	if (!needsQuoting) {
		return normalizedValue;
	}

	return `${QUOTE_CHARACTER}${normalizedValue.replace(/"/g, '""')}${QUOTE_CHARACTER}`;
}

function serializeCsvRow(values: unknown[], delimiter: string): string {
	return values.map((value) => escapeCsvValue(value, delimiter)).join(delimiter);
}

async function writeLine(writeStream: WriteStream, line: string): Promise<void> {
	const canContinue = writeStream.write(line, 'utf8');

	if (!canContinue) {
		await once(writeStream, 'drain');
	}
}

async function closeWriteStream(writeStream: WriteStream): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		const handleError = (error: Error) => reject(error);

		writeStream.once('error', handleError);
		writeStream.end(() => {
			writeStream.off('error', handleError);
			resolve();
		});
	});
}

function createConnectionConfig(credentials: MicrosoftSqlCredentialData): SqlConnectionConfig {
	return {
		server: toRequiredString(credentials.server, 'Server'),
		port: toNumber(credentials.port, 1433),
		database: toRequiredString(credentials.database, 'Database'),
		user: toRequiredString(credentials.user, 'Username'),
		password: toRequiredString(credentials.password, 'Password'),
		connectionTimeout: toNumber(credentials.connectionTimeout, 30000),
		requestTimeout: toNumber(credentials.requestTimeout, 300000),
		options: {
			enableArithAbort: true,
			encrypt: toBoolean(credentials.encrypt, false),
			trustServerCertificate: toBoolean(credentials.skipTlsValidation, true),
			instanceName: toOptionalString(credentials.instanceName),
		},
	};
}

async function streamQueryToCsvFile(
	connectionConfig: SqlConnectionConfig,
	query: string,
	includeHeaders: boolean,
	delimiter: string,
	abortSignal: AbortSignal | undefined,
	targetFilePath: string,
): Promise<CsvFileResult> {
	const pool = new sql.ConnectionPool(connectionConfig);
	const writeStream = createWriteStream(targetFilePath, { encoding: 'utf8' });

	let pendingWrite = Promise.resolve();
	let rowCount = 0;
	let columnCount = 0;
	let recordsetCount = 0;

	const queueLine = async (line: string, onWritten?: () => void): Promise<void> => {
		pendingWrite = pendingWrite.then(async () => {
			await writeLine(writeStream, line);
			onWritten?.();
		});

		return pendingWrite;
	};

	try {
		await pool.connect();

		const request = pool.request();
		request.stream = true;
		request.arrayRowMode = true;

		const abortHandler = () => {
			request?.cancel();
		};

		abortSignal?.addEventListener('abort', abortHandler, { once: true });

		try {
			await new Promise<void>((resolve, reject) => {
				let isSettled = false;

				const rejectOnce = (error: Error): void => {
					if (isSettled) {
						return;
					}

					isSettled = true;
					request?.cancel();
					reject(error);
				};

				request.on('recordset', (columns) => {
					recordsetCount += 1;

					if (recordsetCount > 1) {
						rejectOnce(
							new ApplicationError(
								'The query returned multiple recordsets. This node can export only one result set to CSV.',
							),
						);
						return;
					}

					const columnNames = columns.map((column: SqlColumnMetadata) => column.name);
					columnCount = columnNames.length;

					if (!includeHeaders) {
						return;
					}

					void queueLine(`${serializeCsvRow(columnNames, delimiter)}${LINE_BREAK}`).catch(
						queuedError => rejectOnce(queuedError),
					);
				});

				request.on('row', (row) => {
					void queueLine(`${serializeCsvRow(row, delimiter)}${LINE_BREAK}`, () => {
						rowCount += 1;
					}).catch((queuedError) => rejectOnce(queuedError));
				});

				request.on('error', (error) => {
					rejectOnce(error);
				});

				request.on('done', () => {
					void pendingWrite.then(() => {
						if (isSettled) {
							return;
						}

						isSettled = true;
						resolve();
					}).catch((queuedError) => rejectOnce(queuedError));
				});

				void request.query(query).catch((queryError: Error) => {
					rejectOnce(queryError);
				});
			});
		} finally {
			abortSignal?.removeEventListener('abort', abortHandler);
		}

		await closeWriteStream(writeStream);

		return {
			columnCount,
			rowCount,
			tempFilePath: targetFilePath,
		};
	} catch (error) {
		writeStream.destroy();
		throw error;
	} finally {
		await pool.close().catch(async () => undefined);
	}
}

export class MicrosoftSqlToCsv implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Microsoft SQL to CSV',
		name: 'microsoftSqlToCsv',
		icon: 'file:microsoftSqlToCsv.svg',
		group: ['input'],
		version: [1],
		description: 'Run a Microsoft SQL query in streaming mode and return the result as a CSV file',
		defaults: {
			name: 'Microsoft SQL to CSV',
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		usableAsTool: true,
		credentials: [
			{
				name: 'microsoftSqlCsvApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				required: true,
				default: '',
				typeOptions: {
					rows: 8,
				},
				placeholder: 'SELECT * FROM dbo.Customers',
				description: 'SQL query to execute',
			},
			{
				displayName: 'Include Headers',
				name: 'includeHeaders',
				type: 'boolean',
				default: true,
				description: 'Whether to include the column names as the first CSV row',
			},
			{
				displayName: 'Delimiter',
				name: 'delimiter',
				type: 'string',
				default: DEFAULT_DELIMITER,
				description: 'Character used to separate columns in the generated CSV',
			},
			{
				displayName: 'File Name',
				name: 'fileName',
				type: 'string',
				default: DEFAULT_FILE_NAME,
				description: 'Name of the CSV file that will be returned',
			},
			{
				displayName: 'Binary Property',
				name: 'binaryPropertyName',
				type: 'string',
				default: DEFAULT_BINARY_PROPERTY,
				description: 'Name of the binary property that will contain the generated CSV file',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			let tempDirectoryPath: string | undefined;

			try {
				const query = this.getNodeParameter('query', itemIndex) as string;
				const includeHeaders = this.getNodeParameter('includeHeaders', itemIndex) as boolean;
				const delimiter = this.getNodeParameter('delimiter', itemIndex) as string;
				const requestedFileName = this.getNodeParameter('fileName', itemIndex) as string;
				const binaryPropertyName = (
					this.getNodeParameter('binaryPropertyName', itemIndex) as string
				).trim();

				if (delimiter === '') {
					throw new NodeOperationError(
						this.getNode(),
						'Delimiter cannot be empty.',
						{ itemIndex },
					);
				}

				if (binaryPropertyName === '') {
					throw new NodeOperationError(
						this.getNode(),
						'Binary Property cannot be empty.',
						{ itemIndex },
					);
				}

				const fileName = normalizeFileName(requestedFileName);
				const tempDirectory = await mkdtemp(path.join(tmpdir(), 'n8n-mssql-csv-'));
				const tempFilePath = path.join(tempDirectory, fileName);

				tempDirectoryPath = tempDirectory;

				const credentials =
					await this.getCredentials<MicrosoftSqlCredentialData>('microsoftSqlCsvApi', itemIndex);
				const connectionConfig = createConnectionConfig(credentials);

				const csvFile = await streamQueryToCsvFile(
					connectionConfig,
					query,
					includeHeaders,
					delimiter,
					this.getExecutionCancelSignal(),
					tempFilePath,
				);

				const binaryData = await this.nodeHelpers.copyBinaryFile(
					csvFile.tempFilePath,
					fileName,
					CSV_MIME_TYPE,
				);

				returnData.push({
					json: {
						...items[itemIndex].json,
						microsoftSqlToCsv: {
							columnCount: csvFile.columnCount,
							fileName,
							rowCount: csvFile.rowCount,
						},
					},
					binary: {
						[binaryPropertyName]: binaryData,
					},
					pairedItem: {
						item: itemIndex,
					},
				});
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: {
							error: (error as Error).message,
						},
						pairedItem: {
							item: itemIndex,
						},
					});
				} else if (error instanceof NodeOperationError) {
					throw error;
				} else {
					throw new NodeOperationError(this.getNode(), error as Error, {
						itemIndex,
					});
				}
			} finally {
				if (tempDirectoryPath !== undefined) {
					await rm(tempDirectoryPath, { recursive: true, force: true });
				}
			}
		}

		return [returnData];
	}
}
