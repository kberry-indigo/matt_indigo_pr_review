import { ApolloLink, NextLink, Operation } from 'apollo-link';
import { setContext } from 'apollo-link-context';
import { HttpLink } from 'apollo-link-http';
import { ApolloServer } from 'apollo-server-express';
import DataLoader from 'dataloader';
import express from 'express';
import { GraphQLSchema } from 'graphql';
import { applyMiddleware, IMiddleware, IMiddlewareGenerator } from 'graphql-middleware';
import { introspectSchema, makeExecutableSchema, makeRemoteExecutableSchema, mergeSchemas } from 'graphql-tools';
import { IResolvers } from 'graphql-tools/dist/Interfaces';
import { GraphQLError } from 'graphql/error/GraphQLError';
import * as _ from 'lodash';
import pubsub from 'pubsub-js';

import { Initializable } from '@indigo-ag/common';

import { Context, ErrorReporter, User, Retry, NestedError } from '../core';
import { GraphQLErrorFormatter } from '../core/gql_error_formatter';
import { ServerLifeCycle } from '../core/server';
import { GraphQLAPIConnector } from '../connectors/graphql';

// tslint:disable-next-line:no-var-requires
const DynamicMiddleware = require('dynamic-middleware');

type GraphQLSchemaMap = { [key: string]: GraphQLSchema };

export type GraphQLBaseConfig = {
  schemaString: string;
  remoteSchemaUrls?: string[];
  resolver: IResolvers;
  path: string;
  fetcher?: any;
  middlewares?: Array<IMiddleware | IMiddlewareGenerator<any, any, any>>;
  createDataLoaders?: () => { [k: string]: DataLoader<any, any> };
};

export enum RemoteSchemaStatus {
  ACTIVE = 'ACTIVE',
  INACTIVE = 'INACTIVE'
}

export abstract class GraphQLBase implements Initializable<express.Application> {
  private readonly graphqlMiddleware = DynamicMiddleware.create(new Function());
  private readonly graphqlMiddlewares: Array<IMiddleware | IMiddlewareGenerator<any, any, any>>;
  private readonly path: string;
  private readonly localSchema: GraphQLSchema;
  private readonly remoteSchemas: GraphQLSchemaMap;
  private server: ApolloServer;
  private readonly fetcher: any;
  private readonly createDataLoaders: () => { [k: string]: DataLoader<any, any> };
  private schemaUrls: string[];
  private retryOperations: { [key: string]: Retry };
  private remoteSchemaVersions: { [key: string]: string };

  constructor(config: GraphQLBaseConfig) {
    this.path = config.path;
    this.fetcher = config.fetcher;
    this.graphqlMiddlewares = config.middlewares || [];
    this.localSchema = this.createSchema(config.schemaString, config.resolver);
    this.remoteSchemas = this.createRemoteSchemaMap(config.remoteSchemaUrls);
    this.createDataLoaders = config.createDataLoaders;
    this.schemaUrls = config.remoteSchemaUrls;
    this.retryOperations = {};
    this.remoteSchemaVersions = {};
  }

  get hasRemoteSchema() {
    return Boolean(this.schemaUrls) && Boolean(this.schemaUrls.length);
  }

  public async initialize(app: express.Application) {
    await this.resolveRemoteSchemas(this.remoteSchemas);
    this.refreshMiddleware();
    this.graphqlMiddleware.handler();
    this.server.applyMiddleware({ app, path: this.path });
    return Promise.resolve();
  }

  public isSchemaHealthy() {
    return !this.hasRemoteSchema || this.schemaUrls.every(url => Boolean(this.remoteSchemas[url]));
  }

  public async updateRemoteSchema(url: string, targetVersion?: string) {
    log.info(`Updating remote schema at ${url}`);

    // When passing a schema version if the version matches no need to update
    if (this.isVersionCorrect(url, targetVersion)) {
      return Promise.resolve();
    }

    this.sendFetchingEvent();

    if (await this.addRemoteSchema(url, targetVersion)) {
      this.refreshMiddleware();
    }

    this.shouldSendFetchingCompleted();
    return Promise.resolve();
  }

  public getRemoteSchemaStatus(url: string) {
    return this.remoteSchemas[url] ? RemoteSchemaStatus.ACTIVE : RemoteSchemaStatus.INACTIVE;
  }

  private isVersionCorrect(url: string, targetVersion: string) {
    return Boolean(targetVersion) && this.remoteSchemaVersions[url] === targetVersion;
  }

  private async resolveRemoteSchemas(remoteSchemas: GraphQLSchemaMap) {
    this.sendFetchingEvent();

    await Promise.all(Object.keys(remoteSchemas).map(url => this.addRemoteSchema(url)));

    this.shouldSendFetchingCompleted();

    return Promise.resolve();
  }

  private sendFetchingEvent() {
    this.publish(ServerLifeCycle.REMOTE_SCHEMAS_FETCHING);
  }

  private shouldSendFetchingCompleted() {
    if (this.isSchemaHealthy()) {
      this.publish(ServerLifeCycle.REMOTE_SCHEMAS_FETCHED);
    }
  }

  private requiredApolloConfigVariable(variableName: string) {
    const value = process.env[variableName];

    if (!value) {
      throw new Error(
        `Could not find required ENV variable "${variableName}" that is required when ENGINE_API_KEY is present.`
      );
    }
    return value;
  }

  private async addRemoteSchema(url: string, targetVersion?: string) {
    const schema = await getIntrospectSchema(url, this.fetcher);
    await this.updateRemoteSchemaVersion(url);
    this.remoteSchemas[url] = schema;

    if (targetVersion) {
      this.startRetryOperation(url, targetVersion);
    }

    return schema;
  }

  private refreshMiddleware() {
    log.info(`Refresh graphql middleware`);
    const schemas: GraphQLSchema[] = [];

    Object.keys(this.remoteSchemas).forEach(url => {
      const remoteSchema = this.remoteSchemas[url];
      if (remoteSchema) {
        schemas.push(remoteSchema);
        log.info(`Remote schema url: ${url}`);
        //  TODO: Export printing method from graphql utility in api/schema
        // log.info(`Schema: ${ print(this.remoteSchemas[url]) }`)
      } else {
        this.startRetryOperation(url);
      }
    });
    schemas.push(this.localSchema);
    const schema = schemas.length === 1 ? schemas[0] : mergeSchemas({ schemas });
    const schemaWithMiddleware = applyMiddleware(schema, ...this.graphqlMiddlewares);
    this.server = this.createServer(schemaWithMiddleware);
  }

  private async updateRemoteSchemaVersion(url: string) {
    log.info(`Update schema version for ${url} with current version ${this.remoteSchemaVersions[url]}`);
    const connector = new GraphQLAPIConnector({ customEndpoint: url, logTag: 'Schema Update', urls: {} });
    try {
      const response = await connector.query(Context.createEmpty(), `{ version }`);
      log.info(`Update remote schema version response: ${JSON.stringify(response)}`);
      this.remoteSchemaVersions[url] = response && response.version;
    } catch (e) {
      log.error(`Failed to get remote schema version for ${url}`, e);
    }
  }

  private startRetryOperation(url: string, targetVersion?: string) {
    const shouldRetryForRemoteSchema =
      this.getRemoteSchemaStatus(url) === RemoteSchemaStatus.INACTIVE ||
      (Boolean(targetVersion) && this.remoteSchemaVersions[url] !== targetVersion);

    if (shouldRetryForRemoteSchema) {
      const self = this;
      const updateAttempt = () => self.updateRemoteSchema(url);

      const check = () =>
        (self.getRemoteSchemaStatus(url) === RemoteSchemaStatus.ACTIVE && !Boolean(targetVersion)) ||
        (Boolean(targetVersion) && this.remoteSchemaVersions[url] === targetVersion);

      const tag = `fetch remote schema at ${url}`;

      const onStop = function(retriesExceeded: boolean) {
        if (retriesExceeded) {
          log.error(`Failed to get remote schema for ${url}`);
          throw new NestedError('Ceres failed to get remote schema');
        }
      };

      const retry = new Retry(
        { asyncFunction: updateAttempt, check, tag, onStop },
        { factor: 2, minTimeout: 1000, retries: 5 }
      );

      this.retryOperations[url] = retry;
      retry.start();
    } else {
      const operation = this.retryOperations[url];
      if (operation) {
        log.info(`Stopping ongoing retry for ${url} after successful update`);
        operation.stop();
      }
    }
  }

  private getAuthToken(req: express.Request) {
    if (req.headers && req.headers.authorization) {
      const parts = req.headers.authorization.split(' ');
      if (parts.length === 2) {
        const scheme = parts[0];
        const credentials = parts[1];

        if (/^Bearer$/i.test(scheme)) {
          return credentials;
        }
      }
    }
  }

  private createSchema(schemaString: string, resolver: IResolvers) {
    return makeExecutableSchema({
      logger: {
        log: (message: string) => {
          log.info(message);
        }
      },
      resolverValidationOptions: {
        requireResolversForResolveType: false
      },
      resolvers: resolver,
      typeDefs: schemaString
    });
  }

  private createRemoteSchemaMap(urls: string[]) {
    return urls
      ? urls.reduce((acc: GraphQLSchemaMap, url: string) => {
          acc[url] = undefined;
          return acc;
        }, {})
      : {};
  }

  private createServer(schema: GraphQLSchema) {
    return new ApolloServer({
      ...(process.env.ENGINE_API_KEY
        ? {
            engine: {
              apiKey: process.env.ENGINE_API_KEY,

              // With this config, we ensure that we report our own client headers to source queries.
              generateClientInfo: ({ request }) => {
                if (!_.get(request, 'http.headers')) {
                  // Unknown client/versions will be indicated in the Apollo platform UI.
                  return {};
                }

                // Clients using @indigo-ag/ui-core will have the following headers to identify clients.
                return {
                  clientName: request.http.headers.get('graphql-client-name'),
                  clientVersion: request.http.headers.get('graphql-client-version')
                };
              },

              // We associate query activity with the tagged schema uploaded in CI/CD by matching the schemaTag.
              schemaTag: this.requiredApolloConfigVariable('ENGINE_SCHEMA_TAG')
            }
          }
        : {}),
      context: ({ req }: { req: express.Request }) =>
        new Context({
          context: req.get('IA-Context') || {
            token: this.getAuthToken(req),
            user: User.fromRequest(req.user)
          },
          dataLoaders: this.createDataLoaders ? this.createDataLoaders() : {},
          req
        }),
      formatError: (error: GraphQLError) => {
        let formattedError = error;
        try {
          formattedError = GraphQLErrorFormatter.format(error);
        } catch (err) {
          log.error(`An unexpected error occurred while formatting the GraphQL error`, err);
        }
        const errorMessage = String(formattedError);
        const { originalError } = error;

        // Kind of odd, but originalError can be undefined
        if (_.get(originalError, 'isExpected', false)) {
          log.info(`Trapped expected error: ${errorMessage}\n${formattedError}`);
        } else {
          log.error(errorMessage, formattedError);

          if (ErrorReporter.isInitialized()) {
            ErrorReporter.capture(errorMessage, {
              exception: formattedError,
              extra: {
                path: formattedError.path,
                positions: formattedError.positions,
                source: formattedError.source && formattedError.source.body
              }
            });
          }
        }

        return formattedError;
      },
      introspection: true,
      playground: {
        endpoint: `${this.path}`,
        settings: {
          'editor.cursorShape': 'line',
          'editor.fontFamily': undefined,
          'editor.fontSize': undefined,
          'editor.reuseHeaders': true,
          'editor.theme': undefined,
          'general.betaUpdates': undefined,
          'request.credentials': 'include',
          'tracing.hideTracingResponse': false
        }
      },
      schema,
      tracing: Boolean(process.env.DEBUG_RESOLVER_TRACING)
    });
  }

  private publish(lifeCycleEvent: ServerLifeCycle) {
    pubsub.publish(`IA.api.server.${lifeCycleEvent}`, this);
  }
}

async function getIntrospectSchema(url: string, fetcher: any) {
  // Workaround for https://github.com/apollographql/graphql-tools/issues/1046.
  // Based on https://github.com/apollographql/graphql-tools/issues/1046#issuecomment-457445794.
  const createErrorNormalizationLink = () => {
    return new ApolloLink((operation: Operation, forward?: NextLink) => {
      return forward(operation).map(data => {
        if (data.errors) {
          for (const error of data.errors) {
            if (!(error instanceof Error)) {
              Object.setPrototypeOf(error, Error.prototype);
            }
          }
        }
        return data;
      });
    });
  };

  const createHttpLink = () => {
    return new HttpLink({
      fetch: fetcher,
      uri: url,
      includeExtensions: true
    });
  };

  const createContextLink = () => {
    return setContext((request, context: { graphqlContext: Context }) => {
      return {
        headers: {
          'IA-Context': context.graphqlContext.toIAContextString(),
          'IA-Trace-Id': context.graphqlContext.req.headers['IA-Trace-Id'],
          'IA-Request-Id': context.graphqlContext.req.headers['IA-Request-Id']
        }
      };
    });
  };

  try {
    const schemaDefinition = await introspectSchema(createHttpLink());
    return makeRemoteExecutableSchema({
      link: ApolloLink.from([createErrorNormalizationLink(), createContextLink(), createHttpLink()]),
      schema: schemaDefinition
    });
  } catch (err) {
    log.warn(`Could not introspect remote schema ${url} `);
  }
}
