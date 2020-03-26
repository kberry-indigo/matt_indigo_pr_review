const pubsub = require('pubsub-js');

const {
  Server,
  ServerLifeCycle
} = require('./server');

const { GraphQLBase } = require('../middleware');

class GraphQL extends GraphQLBase {
  constructor() {
    const schemaString = 'type Query { test: String! }';
    const resolver = {
      Query: {
        test: () => 'Indigo AG'
      }
    };

    super({
      schemaUrls: ['foo.bar', 'indigo.ag'],
      schemaString,
      resolver,
      path: '/graphql',
      requireAuth: false
    });
  }
}

function MockConnector() {
  this.initialize = () => {
    return Promise.resolve();
  };
}

describe('IndigoServer', () => {
  let server;

  beforeAll(() => {
    server = new Server([new MockConnector()], [new GraphQL()]);
    server.start({ port: 9999 });

    server.httpServer.address = jest.fn().mockReturnValue({ port: 'TEST' });
  });

  afterAll(() => {
    server.stop();
  });

  test('it should init connectors', () => {
    expect(server.getLifeCycleStatus()).toContain(ServerLifeCycle.CONNECTORS_READY);
  });

  test('it should init middleware', () => {
    expect(server.getLifeCycleStatus()).toContain(ServerLifeCycle.MIDDLEWARE_READY);
  });

  test('server should be unhealthy when fetching remote schema', () => {
    pubsub.publishSync(`IA.api.server.${ServerLifeCycle.REMOTE_SCHEMAS_FETCHING}`);

    expect(server.isApplicationReady).toBeFalsy();
  });

  test('server should be healthy once remote schema has finished fetching', () => {
    pubsub.publishSync(`IA.api.server.${ServerLifeCycle.REMOTE_SCHEMAS_FETCHED}`);

    expect(server.isApplicationReady).toBeTruthy();
  });
});
