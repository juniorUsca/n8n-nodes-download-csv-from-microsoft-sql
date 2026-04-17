import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class MicrosoftSqlCsvApi implements ICredentialType {
	name = 'microsoftSqlCsvApi';

	displayName = 'Microsoft SQL';

	icon = 'file:../nodes/MicrosoftSqlToCsv/microsoftSqlToCsv.svg' as const;

	documentationUrl = 'https://github.com/juniorUsca/n8n-nodes-download-csv-from-microsoft-sql';

	properties: INodeProperties[] = [
		{
			displayName: 'Server',
			name: 'server',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'sqlserver.example.local',
			description: 'Hostname or IP address of the SQL Server instance',
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 1433,
			description: 'TCP port used by SQL Server',
		},
		{
			displayName: 'Database',
			name: 'database',
			type: 'string',
			default: '',
			required: true,
			description: 'Database name to connect to',
		},
		{
			displayName: 'Username',
			name: 'user',
			type: 'string',
			default: '',
			required: true,
			description: 'Username used to authenticate against SQL Server',
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			required: true,
			description: 'Password used to authenticate against SQL Server',
		},
		{
			displayName: 'Instance Name',
			name: 'instanceName',
			type: 'string',
			default: '',
			description: 'Optional SQL Server instance name',
		},
		{
			displayName: 'Encrypt',
			name: 'encrypt',
			type: 'boolean',
			default: false,
			description: 'Whether to encrypt the SQL Server connection',
		},
		{
			displayName: 'Trust Server Certificate',
			name: 'skipTlsValidation',
			type: 'boolean',
			default: true,
			description: 'Whether to trust the server certificate without validating it',
		},
		{
			displayName: 'Connection Timeout (ms)',
			name: 'connectionTimeout',
			type: 'number',
			default: 30000,
			description: 'Maximum time to wait when opening the database connection',
		},
		{
			displayName: 'Request Timeout (ms)',
			name: 'requestTimeout',
			type: 'number',
			default: 300000,
			description: 'Maximum time to wait for the query to finish',
		},
	];
}
