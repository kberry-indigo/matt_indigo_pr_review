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
import _ from 'lodash';

import { Initializable } from '@indigo-ag/common';

import { Context, ErrorReporter, NestedError, User } from '../core';
import { GraphQLErrorFormatter } from '../core/gql_error_formatter';

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

  constructor(config: GraphQLBaseConfig) {
    this.path = config.path;
    this.fetcher = config.fetcher;
    this.graphqlMiddlewares = config.middlewares || [];
    this.localSchema = this.createSchema(config.schemaString, config.resolver);
    this.remoteSchemas = this.createRemoteSchemaMap(config.remoteSchemaUrls);
    this.createDataLoaders = config.createDataLoaders;
    this.schemaUrls = config.remoteSchemaUrls;
  }

  public async initialize(app: express.Application) {
    await this.resolveRemoteSchemas();
    this.refreshMiddleware();
    this.graphqlMiddleware.handler();
    this.server.applyMiddleware({ app, path: this.path });
    return Promise.resolve();
  }

  public isSchemaHealthy() {
    return !this.schemaUrls || this.schemaUrls.every(url => !!this.remoteSchemas[url]);
  }

  public async updateRemoteSchema(url: string) {
    log.info(`Updating remote schema at ${url}`);
    await this.addRemoteSchema(url);
    this.refreshMiddleware();
  }

  public getRemoteSchemaStatus(url: string) {
    return this.remoteSchemas[url] ? RemoteSchemaStatus.ACTIVE : RemoteSchemaStatus.INACTIVE;
  }

  private async resolveRemoteSchemas() {
    await Promise.all(Object.keys(this.remoteSchemas).map(url => this.addRemoteSchema(url)));

    if (this.isSchemaHealthy()) {
      return Promise.resolve();
    }

    return Promise.reject('Unable to resolve all remote schemas. Stopping GraphQL initilization...');
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

  private async addRemoteSchema(url: string) {
    const schema = await getIntrospectSchema(url, this.fetcher);
    this.remoteSchemas[url] = schema;
  }

  private refreshMiddleware() {
    log.info(`Refresh graphql middleware`);
    const schemas: GraphQLSchema[] = [];
    Object.keys(this.remoteSchemas).forEach(url => {
      if (this.remoteSchemas[url]) {
        schemas.push(this.remoteSchemas[url]);
        log.info(`Remote schema url: ${url}`);
        //  TODO: Export printing method from graphql utility in api/schema
        // log.info(`Schema: ${ print(this.remoteSchemas[url]) }`)
      } else {
        log.warn(`Could not resolve schema for url ${url}`);
      }
    });
    schemas.push(this.localSchema);
    const schema = schemas.length === 1 ? schemas[0] : mergeSchemas({ schemas });
    const schemaWithMiddleware = applyMiddleware(schema, ...this.graphqlMiddlewares);
    this.server = this.createServer(schemaWithMiddleware);
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
      // No need to specify a `logger` here; it only logs GraphQL errors,
      // (https://github.com/apollographql/graphql-tools/blob/master/src/makeExecutableSchema.ts#L68)
      // which are already logged by the `ErrorReporter` in the `formatError` callback below.
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
          // Inherits any `isExpected` flag present on the `originalError`
          formattedError = GraphQLErrorFormatter.format(error);
        } catch (err) {
          log.error(`An unexpected error occurred while formatting the GraphQL error`, err);
        }
        const errorMessage = String(formattedError);
        const { originalError } = formattedError; // Can be undefined

        const extra = {
          'GraphQL Body': _.get(formattedError, 'source.body', undefined)
        };

        if (originalError && originalError instanceof NestedError) {
          Object.assign(extra, (originalError as NestedError).getSentryExtraData())
        }

        // Implicitly logs the error to the console
        ErrorReporter.capture(errorMessage, {
          exception: formattedError,
          extra
        });

        return formattedError;
      },
      introspection: true,
      playground: {
        endpoint: `${this.path}`,
        settings: {
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
