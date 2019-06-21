import {
  BasisPrice,
  BidSort,
  CreateGrowerMarketplaceOrderInput,
  CreateHedgeOrderInput,
  Crop,
  CropPricingType,
  Currency,
  FuturesMonthCode,
  LimitOffsetPageInfo,
  MarketOrderProduct,
  MarketOrderStatus,
  MarketplaceOrderOrderByInput,
  MarketplaceOrderSortBy,
  ShippingProvider,
  SortDirection
} from '@indigo-ag/schema';
import { Context, User } from '@indigo-ag/server';
import { UserInputError } from 'apollo-server-errors';
import { factory } from 'factory-girl';
import * as moment from 'moment';
import { generateDummyContext, NON_GMA_USER } from '../../../test/fixtures/context.fixture';
import { databaseTestFramework } from '../../../test/framework/database';
import { createDataLoaderContextForTest } from '../../../test/helpers';
import { MarketplaceBidAPIConnector } from '../../connectors/bid_api/connector';
import { EmailService, UserService } from '../../services';
import { BidAcceptanceDTO } from '../acceptances';
import MarketplaceAcceptanceModel from '../acceptances/acceptance.model';
import { AddressDTO } from '../addresses';
import AddressModel from '../addresses/address.model';
import { DemandOrderDTO, DemandOrderModel } from '../orders';
import { PricingService } from '../pricing';
import { MarketplaceSupplyProfileDTO } from '../supply';
import { GMAService } from '../supply/gma.service';
import MarketplaceSupplyProfileModel from '../supply/profile.model';
import { MarketplaceUserAgreementService } from '../userAgreement';
import { MarketplaceUserAgreementAcceptanceService } from '../userAgreementAcceptance';
import { GrowerOffersForDemandPagedResponseDTO } from './growerOffersForDemandPagedResponse.dto';
import { MarketplaceOrderDAL } from './marketplaceOrder.dal';
import { MarketplaceOrderDTO } from './marketplaceOrder.dto';
import MarketplaceOrderModel from './marketplaceOrder.model';
import {
  createMarketplaceOrderDataLoaders,
  MarketplaceOrderMutations,
  MarketplaceOrderQueries,
  MarketplaceOrderTypeResolvers
} from './marketplaceOrder.resolver';
import { MarketplaceOrderService } from './marketplaceOrder.service';

const globalTestAccountName = 'test name';
const dummyUser: User = NON_GMA_USER;
const dummyContext: Context = generateDummyContext(dummyUser);

describe('MarketplaceOrderResolver', () => {
  beforeAll(async () => {
    GMAService.isContextUserGma = jest.fn().mockImplementation((context: Context) => false);
    return databaseTestFramework.acquireDatabase();
  });

  afterAll(async () => {
    factory.created.clear();
    return databaseTestFramework.releaseDatabase();
  });

  beforeEach(async () => {
    return databaseTestFramework.clearDatabase();
  });

  afterEach(async () => {
    factory.created.clear();
  });

  describe('MarketplaceOrderQueries', () => {
    describe('marketplaceOrder', () => {
      let orderFromDB: MarketplaceOrderModel = null;
      let orderReturnedByApi: MarketplaceOrderDTO = null;
      beforeEach(async () => {
        orderFromDB = await factory.create('MarketplaceOrder');
        orderReturnedByApi = await MarketplaceOrderQueries.marketplaceOrder(
          null,
          { where: { id: orderFromDB.uid } },
          dummyContext
        );
        return;
      });

      it('should return an order', () => {
        expect(orderReturnedByApi).toBeDefined();
      });
    });

    describe('marketplaceOrders', () => {
      let acceptedOrderModel: MarketplaceOrderModel = null;
      let closedOrderModel: MarketplaceOrderModel = null;
      let orderWithKnownSupplyProfile: MarketplaceOrderModel = null;
      let hedgeOrderWithKnownAddress: MarketplaceOrderModel = null;
      let hedgeOrderPartiallyContracted: MarketplaceOrderModel = null;
      let hedgeOrderFullyContracted: MarketplaceOrderModel = null;

      let knownSupplyProfile: MarketplaceSupplyProfileModel = null;
      let knownAddress: AddressModel = null;

      let otcOrders: MarketplaceOrderModel[] = null;
      let hedgeOrders: MarketplaceOrderModel[] = null;

      beforeEach(async () => {
        knownSupplyProfile = await factory.create('MarketplaceSupplyProfile');
        knownAddress = await factory.create('Address', {
          state: 'TX'
        });

        [
          acceptedOrderModel,
          closedOrderModel,
          orderWithKnownSupplyProfile,
          hedgeOrderWithKnownAddress,
          hedgeOrderPartiallyContracted,
          hedgeOrderFullyContracted
        ] = await factory.createMany('MarketplaceOrder', [
          {
            status: MarketOrderStatus.ACCEPTED,
            createdAt: moment()
              .subtract(7, 'days')
              .toDate()
          },
          {
            status: MarketOrderStatus.CLOSED,
            createdAt: moment()
              .subtract(3, 'days')
              .toDate()
          },
          { supplyProfileId: knownSupplyProfile.id, createdAt: moment() },
          { addressId: knownAddress.id, product: MarketOrderProduct.HEDGE, filledQuantity: 0, contractedQuantity: 0 },
          { product: MarketOrderProduct.HEDGE, filledQuantity: 5000, contractedQuantity: 1000 },
          { product: MarketOrderProduct.HEDGE, filledQuantity: 3000, contractedQuantity: 3000, crop: Crop.CLOVER }
        ]);

        otcOrders = [acceptedOrderModel, closedOrderModel, orderWithKnownSupplyProfile];
        hedgeOrders = [hedgeOrderWithKnownAddress, hedgeOrderPartiallyContracted, hedgeOrderFullyContracted];
      });

      it('should return all OTC orders if no filter is passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(null, { where: {} }, dummyContext);
        expect(ordersReturnedByApi.length).toEqual(otcOrders.length);
        expect(ordersReturnedByApi.every(order => order.product === MarketOrderProduct.OTC));
      });

      it('should filter by hedge product when specified', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] }
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(hedgeOrders.length);
        expect(ordersReturnedByApi.every(order => order.product === MarketOrderProduct.HEDGE));
      });

      it('should sort the orders so that the newest order is first, if no sort is set', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {} },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is first, if sort by created at in desc order', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, sort: { sortBy: MarketplaceOrderSortBy.DATE_CREATED, sortDirection: SortDirection.DESC } },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is last, if sort by created at in asc order', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, sort: { sortBy: MarketplaceOrderSortBy.DATE_CREATED, sortDirection: SortDirection.DESC } },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should filter by status and return only closed orders when the filter is set to closed', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { status: MarketOrderStatus.CLOSED } },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [closedOrderReturnedByApi] = ordersReturnedByApi;
        expect(closedOrderModel.uid).toEqual(closedOrderReturnedByApi.id);
      });

      it('should also filter out expired offers if filtering on the open status', async () => {
        const expiredOrder = await factory.create('MarketplaceOrder', {
          expiresAt: moment()
            .subtract(7, 'days')
            .toDate()
        });
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { status: MarketOrderStatus.OPEN } },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        expect(ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === expiredOrder.uid)).toBeFalsy();
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
      });

      it('should return OPEN || PENDING || WORKING orders when filtering by status = OPEN', async () => {
        await factory.createMany('MarketplaceOrder', [
          { status: MarketOrderStatus.OPEN },
          { status: MarketOrderStatus.PENDING },
          { status: MarketOrderStatus.WORKING }
        ]);

        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { status: MarketOrderStatus.OPEN } },
          dummyContext
        );

        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.OPEN)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.PENDING)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.WORKING)
        ).toBeTruthy();
      });

      it('should return ACCEPTED and FILLED orders when filtering by status = ACCEPTED', async () => {
        await factory.createMany('MarketplaceOrder', [
          { status: MarketOrderStatus.ACCEPTED },
          { status: MarketOrderStatus.FILLED }
        ]);

        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { status: MarketOrderStatus.ACCEPTED } },
          dummyContext
        );

        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.ACCEPTED)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.FILLED)
        ).toBeTruthy();
      });

      it('should not throw an error if the status_in filter is populated', async () => {
        expect.assertions(1);

        expect(() => {
          MarketplaceOrderQueries.marketplaceOrders(
            null,
            {
              where: {
                status_in: [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED]
              }
            },
            dummyContext
          );
        }).not.toThrow();
      });

      it('should filter by the list of uids passed', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { ids: [acceptedOrderModel.uid, closedOrderModel.uid] } },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(2);
        const returnedUids = ordersReturnedByApi.map(orderDto => orderDto.id);
        expect(returnedUids).toContain(acceptedOrderModel.uid);
        expect(returnedUids).toContain(closedOrderModel.uid);
      });

      it('should filter by the supply profile uid passed', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { supplyProfileId: knownSupplyProfile.uid } },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [orderDto] = ordersReturnedByApi;
        expect(orderWithKnownSupplyProfile.uid).toEqual(orderDto.id);
      });

      it('should filter by the crop passed', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: {
              crop: hedgeOrderFullyContracted.crop,
              product_in: [MarketOrderProduct.HEDGE]
            }
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [orderDto] = ordersReturnedByApi;
        expect(hedgeOrderFullyContracted.uid).toEqual(orderDto.id);
      });
    });

    describe('marketplaceOrders with listAll', () => {
      let acceptedOrderModel: MarketplaceOrderModel = null;
      let closedOrderModel: MarketplaceOrderModel = null;
      let orderWithKnownSupplyProfile: MarketplaceOrderModel = null;
      let knownSupplyProfile: MarketplaceSupplyProfileModel = null;
      let otcOrders: MarketplaceOrderModel[] = null;
      beforeEach(async () => {
        knownSupplyProfile = await factory.create('MarketplaceSupplyProfile');
        [acceptedOrderModel, closedOrderModel, orderWithKnownSupplyProfile] = await factory.createMany(
          'MarketplaceOrder',
          [
            {
              status: MarketOrderStatus.ACCEPTED,
              createdAt: moment()
                .subtract(7, 'days')
                .toDate()
            },
            {
              status: MarketOrderStatus.CLOSED,
              createdAt: moment()
                .subtract(3, 'days')
                .toDate()
            },
            {
              supplyProfileId: knownSupplyProfile.id,
              createdAt: moment(),
              crop: Crop.CLOVER
            }
          ]
        );
        otcOrders = [acceptedOrderModel, closedOrderModel, orderWithKnownSupplyProfile];
      });

      it('should return all 3 orders if no filter is passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(null, { where: {} }, dummyContext);
        expect(ordersReturnedByApi.length).toEqual(otcOrders.length);
      });

      it('should sort the orders so that the newest order is first, if no sort is set', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC] },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should return orders paginated (1. page)', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 0, limit: 1 } },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
      });

      it('should return orders paginated (2. page)', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 1, limit: 1 } },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(closedOrderModel.uid);
      });

      it('should return orders paginated (3. page)', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 2, limit: 1 } },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is first, if sort by created at in desc order', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC] },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is last, if sort by created at in asc order', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC] },
          dummyContext
        );
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should filter by status and return only closed orders when the filter is set to closed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          { where: { status: MarketOrderStatus.CLOSED } },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [closedOrderReturnedByApi] = ordersReturnedByApi;
        expect(closedOrderModel.uid).toEqual(closedOrderReturnedByApi.id);
      });

      it('should also filter out expired offers if filtering on the open status', async () => {
        const expiredOrder = await factory.create('MarketplaceOrder', {
          expiresAt: moment()
            .subtract(7, 'days')
            .toDate()
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { status: MarketOrderStatus.OPEN, expiresAt_gt: moment().toISOString() },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        expect(ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === expiredOrder.uid)).toBeFalsy();
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
      });

      it('should return OPEN || PENDING || WORKING orders when filtering by status = OPEN', async () => {
        await factory.createMany('MarketplaceOrder', [
          { status: MarketOrderStatus.OPEN },
          { status: MarketOrderStatus.PENDING },
          { status: MarketOrderStatus.WORKING }
        ]);

        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { status: MarketOrderStatus.OPEN },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );

        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.OPEN)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.PENDING)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.WORKING)
        ).toBeTruthy();
      });

      it('should return ACCEPTED and FILLED orders when filtering by status = ACCEPTED', async () => {
        const filledOrder = await factory.create('MarketplaceOrder', {
          status: MarketOrderStatus.FILLED
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { status: MarketOrderStatus.ACCEPTED },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );

        expect(ordersReturnedByApi.length).toEqual(2);
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === acceptedOrderModel.uid)
        ).toBeTruthy();
        expect(ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === filledOrder.uid)).toBeTruthy();
      });

      it('should return filter by the list of status passed', async () => {
        const filledOrder = await factory.create('MarketplaceOrder', {
          status: MarketOrderStatus.FILLED
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: {
              status_in: [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED]
            },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );

        expect(ordersReturnedByApi.length).toEqual(2);
        expect(
          ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === acceptedOrderModel.uid)
        ).toBeTruthy();
        expect(ordersReturnedByApi.find((order: MarketplaceOrderDTO) => order.id === filledOrder.uid)).toBeTruthy();
      });

      it('should throw an error if both the @deprecated status and status_in filters are used at the same time', async () => {
        expect.assertions(2);

        let caughtError: UserInputError = null;
        try {
          await MarketplaceOrderQueries.marketplaceOrders(
            null,
            {
              where: {
                status_in: [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED],
                status: MarketOrderStatus.OPEN
              },
              orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
            },
            dummyContext
          );
        } catch (error) {
          caughtError = error;
        }
        expect(caughtError).toBeInstanceOf(UserInputError);
        expect(caughtError.message).toEqual(
          `The endpoint does not support the use of both the status_in and the deprecated status filter at the same time.`
        );
      });

      it('should filter by the list of uids passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { ids: [acceptedOrderModel.uid, closedOrderModel.uid] },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(2);
        const returnedUids = ordersReturnedByApi.map(orderDto => orderDto.id);
        expect(returnedUids).toContain(acceptedOrderModel.uid);
        expect(returnedUids).toContain(closedOrderModel.uid);
      });

      it('should filter by the supply profile uid passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: { supplyProfileId: knownSupplyProfile.uid },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [orderDto] = ordersReturnedByApi;
        expect(orderWithKnownSupplyProfile.uid).toEqual(orderDto.id);
      });

      it('should filter by the crop passed', async () => {
        const ordersReturnedByApi: MarketplaceOrderDTO[] = await MarketplaceOrderQueries.marketplaceOrders(
          null,
          {
            where: {
              crop: orderWithKnownSupplyProfile.crop,
              product_in: [MarketOrderProduct.OTC]
            },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC]
          },
          dummyContext
        );
        expect(ordersReturnedByApi.length).toEqual(1);
        const [orderDto] = ordersReturnedByApi;
        expect(orderWithKnownSupplyProfile.uid).toEqual(orderDto.id);
      });
    });

    describe('marketplaceOrdersPaginated', () => {
      let defaultOrderBy: MarketplaceOrderOrderByInput = MarketplaceOrderOrderByInput.createdAt_ASC;
      let defaultPagination: LimitOffsetPageInfo = {
        limit: 20,
        offset: 0
      };

      let acceptedOrderModel: MarketplaceOrderModel = null;
      let closedOrderModel: MarketplaceOrderModel = null;
      let orderWithKnownSupplyProfile: MarketplaceOrderModel = null;
      let hedgeOrderWithKnownAddress: MarketplaceOrderModel = null;
      let hedgeOrderPartiallyContracted: MarketplaceOrderModel = null;
      let hedgeOrderFullyContracted: MarketplaceOrderModel = null;

      let knownSupplyProfile: MarketplaceSupplyProfileModel = null;
      let knownAddress: AddressModel = null;

      let knownReferenceId: string = null;

      let otcOrders: MarketplaceOrderModel[] = null;
      let hedgeOrders: MarketplaceOrderModel[] = null;
      const futuresReferencePrice = 4.52;

      beforeEach(async () => {
        knownSupplyProfile = await factory.create('MarketplaceSupplyProfile');
        knownAddress = await factory.create('Address', {
          state: 'TX'
        });
        [acceptedOrderModel, closedOrderModel, orderWithKnownSupplyProfile] = await factory.createMany(
          'MarketplaceOrder',
          [
            {
              status: MarketOrderStatus.ACCEPTED,
              createdAt: moment()
                .subtract(7, 'days')
                .toDate()
            },
            {
              status: MarketOrderStatus.CLOSED,
              createdAt: moment()
                .subtract(3, 'days')
                .toDate()
            },
            { supplyProfileId: knownSupplyProfile.id, createdAt: moment() }
          ]
        );

        [
          hedgeOrderWithKnownAddress,
          hedgeOrderPartiallyContracted,
          hedgeOrderFullyContracted
        ] = await factory.createMany('MarketplaceOrderWithFloatingBasis', [
          {
            addressId: knownAddress.id,
            product: MarketOrderProduct.HEDGE,
            filledQuantity: 0,
            contractedQuantity: 0,
            status: MarketOrderStatus.WORKING
          },
          {
            product: MarketOrderProduct.HEDGE,
            filledQuantity: 5000,
            contractedQuantity: 1000,
            status: MarketOrderStatus.WORKING,
            futuresReferencePrice
          },
          {
            product: MarketOrderProduct.HEDGE,
            filledQuantity: 3000,
            contractedQuantity: 3000,
            status: MarketOrderStatus.FILLED,
            crop: Crop.CLOVER,
            futuresReferencePrice,
            basisMonthCode: FuturesMonthCode.G,
            basisYear: 2020
          }
        ]);

        otcOrders = [acceptedOrderModel, closedOrderModel, orderWithKnownSupplyProfile];
        hedgeOrders = [hedgeOrderWithKnownAddress, hedgeOrderPartiallyContracted, hedgeOrderFullyContracted];

        knownReferenceId = acceptedOrderModel.referenceId;
      });

      it('should return all OTC orders if no filter is passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [defaultOrderBy], pagination: defaultPagination },
          dummyContext
        );
        expect(ordersReturnedByApi.total).toEqual(otcOrders.length);
        expect(ordersReturnedByApi.data.length).toEqual(otcOrders.length);
      });

      it('should handle an undefined orderBy being passed in by graphQL', async () => {
        /*
        This test is to ensure we don't throw an error when we check if the orderBy.includes certain computed columns
         */
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: undefined, pagination: defaultPagination },
          dummyContext
        );
        expect(ordersReturnedByApi.total).toEqual(otcOrders.length);
        expect(ordersReturnedByApi.data.length).toEqual(otcOrders.length);
      });

      it('should handle a null orderBy being passed in by graphQL', async () => {
        /*
        This test is to ensure we don't throw an error when we check if the orderBy.includes certain computed columns
         */
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: null, pagination: defaultPagination },
          dummyContext
        );
        expect(ordersReturnedByApi.total).toEqual(otcOrders.length);
        expect(ordersReturnedByApi.data.length).toEqual(otcOrders.length);
      });

      it('should sort the orders so that the newest order is first, if no sort is set', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: defaultPagination },
          dummyContext
        );
        const ordersReturnedByApi = ordersPage.data;
        expect(ordersReturnedByApi[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersReturnedByApi[1].id).toEqual(closedOrderModel.uid);
        expect(ordersReturnedByApi[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should return orders paginated (1. page)', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 0, limit: 1 } },
          dummyContext
        );
        expect(ordersPage.data[0].id).toEqual(orderWithKnownSupplyProfile.uid);
      });

      it('should return orders paginated (2. page)', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 1, limit: 1 } },
          dummyContext
        );
        expect(ordersPage.data[0].id).toEqual(closedOrderModel.uid);
      });

      it('should return orders paginated (3. page)', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: { offset: 2, limit: 1 } },
          dummyContext
        );
        expect(ordersPage.data[0].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is first, if sort by created at in desc order', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: defaultPagination },
          dummyContext
        );
        expect(ordersPage.data[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersPage.data[1].id).toEqual(closedOrderModel.uid);
        expect(ordersPage.data[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should sort the orders so that the newest order is last, if sort by created at in asc order', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: {}, orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC], pagination: defaultPagination },
          dummyContext
        );
        expect(ordersPage.data[0].id).toEqual(orderWithKnownSupplyProfile.uid);
        expect(ordersPage.data[1].id).toEqual(closedOrderModel.uid);
        expect(ordersPage.data[2].id).toEqual(acceptedOrderModel.uid);
      });

      it('should filter by status and return only closed orders when the filter is set to closed', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          { where: { status: MarketOrderStatus.CLOSED }, orderBy: [defaultOrderBy], pagination: defaultPagination },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data.length).toEqual(1);
        const [closedOrderReturnedByApi] = ordersPage.data;
        expect(closedOrderModel.uid).toEqual(closedOrderReturnedByApi.id);
      });

      it('should also filter out expired offers if filtering on the open status', async () => {
        const expiredOrder = await factory.create('MarketplaceOrder', {
          expiresAt: moment()
            .subtract(7, 'days')
            .toDate()
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { status: MarketOrderStatus.OPEN, expiresAt_gt: moment().toISOString() },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersReturnedByApi.total).toEqual(1);
        expect(ordersReturnedByApi.data.length).toEqual(1);
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.id === expiredOrder.uid)
        ).toBeFalsy();
        expect(ordersReturnedByApi.data[0].id).toEqual(orderWithKnownSupplyProfile.uid);
      });

      it('should return OPEN || PENDING || WORKING orders when filtering by status = OPEN', async () => {
        await factory.createMany('MarketplaceOrder', [
          { status: MarketOrderStatus.OPEN },
          { status: MarketOrderStatus.PENDING },
          { status: MarketOrderStatus.WORKING }
        ]);

        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { status: MarketOrderStatus.OPEN },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.OPEN)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.PENDING)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.status === MarketOrderStatus.WORKING)
        ).toBeTruthy();
      });

      it('should return ACCEPTED and FILLED orders when filtering by status = ACCEPTED', async () => {
        const filledOrder = await factory.create('MarketplaceOrder', {
          status: MarketOrderStatus.FILLED
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { status: MarketOrderStatus.ACCEPTED },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(ordersReturnedByApi.total).toEqual(2);
        expect(ordersReturnedByApi.data.length).toEqual(2);
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.id === acceptedOrderModel.uid)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.id === filledOrder.uid)
        ).toBeTruthy();
      });

      it('should return filter by the list of status passed', async () => {
        const filledOrder = await factory.create('MarketplaceOrder', {
          status: MarketOrderStatus.FILLED
        });
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: {
              status_in: [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED]
            },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(ordersReturnedByApi.total).toEqual(2);
        expect(ordersReturnedByApi.data.length).toEqual(2);
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.id === acceptedOrderModel.uid)
        ).toBeTruthy();
        expect(
          ordersReturnedByApi.data.find((order: MarketplaceOrderDTO) => order.id === filledOrder.uid)
        ).toBeTruthy();
      });

      it('should throw an error if both the @deprecated status and status_in filters are used at the same time', async () => {
        expect.assertions(2);

        let caughtError: UserInputError = null;
        try {
          await MarketplaceOrderQueries.marketplaceOrdersPaginated(
            null,
            {
              where: {
                status_in: [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED],
                status: MarketOrderStatus.OPEN
              },
              orderBy: [MarketplaceOrderOrderByInput.createdAt_DESC],
              pagination: defaultPagination
            },
            dummyContext
          );
        } catch (error) {
          caughtError = error;
        }
        expect(caughtError).toBeInstanceOf(UserInputError);
        expect(caughtError.message).toEqual(
          `The endpoint does not support the use of both the status_in and the deprecated status filter at the same time.`
        );
      });

      it('should filter by the list of uids passed', async () => {
        const ordersReturnedByApi = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { ids: [acceptedOrderModel.uid, closedOrderModel.uid] },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersReturnedByApi.total).toEqual(2);
        expect(ordersReturnedByApi.data.length).toEqual(2);
        const returnedUids = ordersReturnedByApi.data.map(orderDto => orderDto.id);
        expect(returnedUids).toContain(acceptedOrderModel.uid);
        expect(returnedUids).toContain(closedOrderModel.uid);
      });

      it('should filter by the supply profile uid passed', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { supplyProfileId: knownSupplyProfile.uid },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data.length).toEqual(1);
        const [orderDto] = ordersPage.data;
        expect(orderWithKnownSupplyProfile.uid).toEqual(orderDto.id);
      });

      it('should filter by state', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { state: knownAddress.state, product_in: [MarketOrderProduct.OTC, MarketOrderProduct.HEDGE] },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        const [orderDto] = ordersPage.data;
        expect(hedgeOrderWithKnownAddress.uid).toEqual(orderDto.id);
      });

      it('should filter by hedge product when specified', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(ordersPage.data.every(order => order.product === MarketOrderProduct.HEDGE));
      });

      it('should filter by referenceId when specified', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { referenceId: knownReferenceId },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data[0].referenceId).toEqual(knownReferenceId);
        expect(ordersPage.data[0].id).toEqual(acceptedOrderModel.uid);
      });

      it('should return an empty list if referenceId specified does not match an existing order', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { referenceId: 'none' },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(0);
        expect(ordersPage.data).toEqual([]);
      });

      it('should filter by the crop passed', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: {
              crop: hedgeOrderFullyContracted.crop,
              product_in: [MarketOrderProduct.HEDGE]
            },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data[0].crop).toEqual(hedgeOrderFullyContracted.crop);
        expect(ordersPage.data[0].id).toEqual(hedgeOrderFullyContracted.uid);
      });

      it('should return only orders with availableToContractQuantity when hasQuantityAvailableToContract = true', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE], hasQuantityAvailableToContract: true },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data.every(order => order.product === MarketOrderProduct.HEDGE));
        expect(ordersPage.data.every(order => order.availableToContractQuantity > 0));
      });

      it('should not return orders with availableToContractQuantity when hasQuantityAvailableToContract = false', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE], hasQuantityAvailableToContract: false },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(2);
        expect(ordersPage.data.every(order => order.product === MarketOrderProduct.HEDGE));
        expect(ordersPage.data.every(order => order.availableToContractQuantity <= 0));
      });

      it(`Should filter by basisMonthCode and basisYear`, async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: {
              product_in: [MarketOrderProduct.HEDGE],
              basisMonthCode: hedgeOrderFullyContracted.basisMonthCode,
              basisYear: hedgeOrderFullyContracted.basisYear
            },
            orderBy: [defaultOrderBy],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(1);
        expect(ordersPage.data[0].id).toEqual(hedgeOrderFullyContracted.uid);
      });

      it('should order by contracted quantity ASC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [MarketplaceOrderOrderByInput.contractedQuantity_ASC],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[2].id);
      });

      it('should order by contracted quantity DESC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [MarketplaceOrderOrderByInput.contractedQuantity_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[2].id);
      });

      it('should order by filled quantity ASC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [MarketplaceOrderOrderByInput.filledQuantity_ASC],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[2].id);
      });

      it('should order by filled quantity DESC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [MarketplaceOrderOrderByInput.filledQuantity_DESC],
            pagination: defaultPagination
          },
          dummyContext
        );
        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[2].id);
      });

      it('should order by availableToContractQuantity ASC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [
              MarketplaceOrderOrderByInput.availableToContractQuantity_ASC,
              //Added filledQuantity_ASC as we have some orders with the same availableToContractQuantity value
              MarketplaceOrderOrderByInput.filledQuantity_ASC
            ],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[2].id);
      });

      it('should order by availableToContractQuantity DESC', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [
              MarketplaceOrderOrderByInput.availableToContractQuantity_DESC,
              //Added filledQuantity_ASC as we have some orders with the same availableToContractQuantity value
              MarketplaceOrderOrderByInput.filledQuantity_ASC
            ],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[2].id);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[1].id);
      });

      it('should maintain the order of sort operators when an availableToContractQuantity operator is used', async () => {
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [
              MarketplaceOrderOrderByInput.filledQuantity_ASC,
              MarketplaceOrderOrderByInput.availableToContractQuantity_DESC,
              MarketplaceOrderOrderByInput.contractedQuantity_ASC
            ],
            pagination: defaultPagination
          },
          dummyContext
        );

        expect(ordersPage.total).toEqual(hedgeOrders.length);
        expect(hedgeOrderWithKnownAddress.uid).toEqual(ordersPage.data[0].id);
        expect(hedgeOrderFullyContracted.uid).toEqual(ordersPage.data[1].id);
        expect(hedgeOrderPartiallyContracted.uid).toEqual(ordersPage.data[2].id);
      });

      it('Should return futuresReferencePrice if set on marketplaceOrder', async () => {
        expect.assertions(1);
        const ordersPage = await MarketplaceOrderQueries.marketplaceOrdersPaginated(
          null,
          {
            where: { ids: [hedgeOrderFullyContracted.uid], product_in: [MarketOrderProduct.HEDGE] },
            orderBy: [MarketplaceOrderOrderByInput.createdAt_ASC],
            pagination: { limit: 1, offset: 0 }
          },
          dummyContext
        );
        const basisPrice = ordersPage.data[0].price as BasisPrice;
        expect(basisPrice.lockedInPrice).toEqual(futuresReferencePrice);
      });
    });

    describe('growerOffersForDemand', () => {
      const mockDseServiceResponse = {
        growerOffersForDemand: {
          total: 4,
          hasNextPage: false,
          hasPreviousPage: false,
          growerOffers: [
            {
              minQuantity: 100000,
              maxQuantity: 100000,
              shippingDistance: 1.129652827487473,
              shippingProvider: 'SELLER',
              supplySource: {
                growerOfferId: 'rJFW_59fN'
              }
            },
            {
              minQuantity: 100000,
              maxQuantity: 100000,
              shippingDistance: 1.129652827487473,
              shippingProvider: 'SELLER',
              supplySource: {
                growerOfferId: 'SJTOsqcMV'
              }
            },
            {
              minQuantity: 100000,
              maxQuantity: 100000,
              shippingDistance: 1.129652827487473,
              shippingProvider: 'SELLER',
              supplySource: {
                growerOfferId: 'rJX0Aq5z4'
              }
            },
            {
              minQuantity: 1000,
              maxQuantity: 1000,
              shippingDistance: 51.17488865028235,
              shippingProvider: 'SELLER',
              supplySource: {
                growerOfferId: 'Bkz4Ry-GV'
              }
            }
          ]
        }
      };

      let originalGetGrowerOffersForDemand = MarketplaceBidAPIConnector.prototype.getGrowerOffersForDemand;
      let mockedGrowerOffersForDemandDseService = jest.fn(() => {
        return Promise.resolve(mockDseServiceResponse);
      });
      let growerOffersForDemandRespnse: GrowerOffersForDemandPagedResponseDTO = null;

      beforeEach(async () => {
        MarketplaceBidAPIConnector.prototype.getGrowerOffersForDemand = mockedGrowerOffersForDemandDseService;
        const supplyProfileAddress: AddressModel = await factory.create('Address');
        growerOffersForDemandRespnse = await MarketplaceOrderQueries.growerOffersForDemand(
          null,
          {
            where: {
              contractType_in: [CropPricingType.CASH],
              location: {
                latitude: supplyProfileAddress.latitude,
                longitude: supplyProfileAddress.longitude
              },
              shippingProvider_in: [ShippingProvider.SELLER],
              shippingDistanceMiles_lt: 100
            },
            orderBy: BidSort.NEWEST,
            offset: 0,
            limit: 100,
            sortDirection: SortDirection.ASC
          },
          dummyContext
        );
      });

      afterEach(() => {
        MarketplaceBidAPIConnector.prototype.getGrowerOffersForDemand = originalGetGrowerOffersForDemand;
      });

      it('should call the DSE service', () => {
        expect(mockedGrowerOffersForDemandDseService).toBeCalledTimes(1);
      });

      it('should return the DS service response', () => {
        let growerOffersForDemandMock = mockDseServiceResponse.growerOffersForDemand;
        let normalizedMockOffers = growerOffersForDemandMock.growerOffers.map(offer => {
          const { supplySource, ...restofAttributes } = offer;
          return {
            id: supplySource.growerOfferId,
            ...restofAttributes
          };
        });

        expect(mockDseServiceResponse.growerOffersForDemand.growerOffers.length).toEqual(
          growerOffersForDemandRespnse.growerOffers.length
        );
        expect(normalizedMockOffers).toEqual(growerOffersForDemandRespnse.growerOffers);
        expect(growerOffersForDemandMock.total).toEqual(growerOffersForDemandRespnse.total);
        expect(growerOffersForDemandMock.hasNextPage).toEqual(growerOffersForDemandRespnse.hasNextPage);
        expect(growerOffersForDemandMock.hasPreviousPage).toEqual(growerOffersForDemandRespnse.hasPreviousPage);
      });
    });
  });

  describe('MarketplaceOrderMutations', () => {
    let getAccountUserEdgesByAccountIdMock = jest.fn(() => [{ node: { email: dummyUser.email } }]);
    let originalGetAccountUserEdgesByAccountId = UserService.getAccountUserEdgesByAccountId;
    let getAccountMock = jest.fn(() => {
      return Promise.resolve({
        id: dummyUser.primaryAccountId,
        name: `${globalTestAccountName}`,
        owner: {
          email: dummyUser.email,
          id: dummyUser.id
        }
      });
    });
    let originalGetAccountMock = UserService.getAccount;

    let originalfindLatestAgreementForState = MarketplaceUserAgreementService.findLatestForState;
    let originalFindAcceptedAgreement = MarketplaceUserAgreementAcceptanceService.find;
    let latestAgreementSigned = true;

    const mockUserAgreement = factory.create('MarketplaceUserAgreement');
    const findLatestAgreementForStateMock = jest.fn().mockImplementation(() => {
      return mockUserAgreement;
    });
    const findAcceptedAgreementMock = jest.fn().mockImplementation(() => {
      // Return no matching agreement when simulating that the latest agreement wasn't signed.
      return latestAgreementSigned ? mockUserAgreement : undefined;
    });

    beforeEach(() => {
      UserService.getAccountUserEdgesByAccountId = getAccountUserEdgesByAccountIdMock;
      UserService.getAccount = getAccountMock;
      MarketplaceUserAgreementService.findLatestForState = findLatestAgreementForStateMock;
      MarketplaceUserAgreementAcceptanceService.find = findAcceptedAgreementMock;
      latestAgreementSigned = true;
    });

    afterEach(() => {
      UserService.getAccountUserEdgesByAccountId = originalGetAccountUserEdgesByAccountId;
      UserService.getAccount = originalGetAccountMock;
      MarketplaceUserAgreementService.findLatestForState = originalfindLatestAgreementForState;
      MarketplaceUserAgreementAcceptanceService.find = originalFindAcceptedAgreement;
    });

    describe('closeGrowerMarketplaceOrder', () => {
      let sendGrowerMarketplaceOrderClosedMock = jest.fn(() => Promise.resolve());
      let originalSendGrowerMarketplaceOrderClosed = EmailService.prototype.sendMarketplaceOrderClosed;

      beforeAll(async () => {
        GMAService.getAssociatedGMAUser = jest.fn().mockImplementation((accountUid: string, context: Context) => {
          return undefined;
        });
      });

      beforeEach(async () => {
        sendGrowerMarketplaceOrderClosedMock.mockClear();
        EmailService.prototype.sendMarketplaceOrderClosed = sendGrowerMarketplaceOrderClosedMock;
      });

      afterEach(() => {
        EmailService.prototype.sendMarketplaceOrderClosed = originalSendGrowerMarketplaceOrderClosed;
      });

      describe('Closing Open Grower Offer', () => {
        let openOrderModel: MarketplaceOrderModel = null;
        let orderReturnedByApi: MarketplaceOrderDTO = null;
        beforeEach(async () => {
          openOrderModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.OPEN });
          orderReturnedByApi = await MarketplaceOrderMutations.closeGrowerMarketplaceOrder(
            null,
            { input: { id: openOrderModel.uid } },
            dummyContext
          );
          return openOrderModel.reload();
        });

        it('should close the specified order in the databasae', () => {
          expect(openOrderModel.status).toEqual(MarketOrderStatus.CLOSED);
        });

        it('should return the closed order', () => {
          expect(orderReturnedByApi.id).toEqual(openOrderModel.uid);
          expect(orderReturnedByApi.status).toEqual(MarketOrderStatus.CLOSED);
        });

        it('should send a closed email', () => {
          const NUM_EMAILS_SENT_INCLUDING_GROWER_AND_SUPPORT_TEAM = 2;
          expect(sendGrowerMarketplaceOrderClosedMock.mock.calls.length).toEqual(
            NUM_EMAILS_SENT_INCLUDING_GROWER_AND_SUPPORT_TEAM
          );
        });
      });

      describe('Closing already closed grower offer', () => {
        let closedOrderModel: MarketplaceOrderModel = null;
        let orderReturnedByApi: MarketplaceOrderDTO = null;
        let originalDALUpdateGrowerOffer = MarketplaceOrderDAL.updateGrowerOffer;
        const mockDALUpdateGrowerOffer = jest.fn();
        beforeEach(async () => {
          mockDALUpdateGrowerOffer.mockClear();
          MarketplaceOrderDAL.updateGrowerOffer = mockDALUpdateGrowerOffer;
          closedOrderModel = await factory.create('MarketplaceOrder', {
            status: MarketOrderStatus.CLOSED
          });
          orderReturnedByApi = await MarketplaceOrderMutations.closeGrowerMarketplaceOrder(
            null,
            { input: { id: closedOrderModel.uid } },
            dummyContext
          );
          await closedOrderModel.reload();
        });

        afterEach(() => {
          MarketplaceOrderDAL.updateGrowerOffer = originalDALUpdateGrowerOffer;
        });

        it('should not update the database', () => {
          expect(mockDALUpdateGrowerOffer.mock.calls.length).toEqual(0);
        });

        it('should not send any emails', () => {
          expect(sendGrowerMarketplaceOrderClosedMock.mock.calls.length).toEqual(0);
        });

        it('should return an untouched marketplace order model', () => {
          expect(orderReturnedByApi.updatedAt).toEqual(closedOrderModel.updatedAt.toISOString());
        });
      });

      describe('Closing an accepted order', () => {
        let acceptedOrderModel: MarketplaceOrderModel = null;
        let originalDALUpdateGrowerOffer = MarketplaceOrderDAL.updateGrowerOffer;
        const mockDALUpdateGrowerOffer = jest.fn();
        let cantCancelAcceptedOrderError: Error = null;
        beforeEach(async () => {
          mockDALUpdateGrowerOffer.mockClear();
          MarketplaceOrderDAL.updateGrowerOffer = mockDALUpdateGrowerOffer;
          acceptedOrderModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.ACCEPTED });

          try {
            await MarketplaceOrderMutations.closeGrowerMarketplaceOrder(
              null,
              { input: { id: acceptedOrderModel.uid } },
              dummyContext
            );
          } catch (err) {
            cantCancelAcceptedOrderError = err;
          }
        });

        afterEach(() => {
          MarketplaceOrderDAL.updateGrowerOffer = originalDALUpdateGrowerOffer;
        });

        it('should not update the database', () => {
          expect(mockDALUpdateGrowerOffer.mock.calls.length).toEqual(0);
        });

        it('should not send any emails', () => {
          expect(sendGrowerMarketplaceOrderClosedMock.mock.calls.length).toEqual(0);
        });

        it('should throw an error', () => {
          expect(cantCancelAcceptedOrderError.message).toEqual(
            `Marketplace order ${acceptedOrderModel.uid} cannot be closed because it has been accepted.`
          );
        });
      });
    });

    describe('createGrowerMarketplaceOrder', () => {
      let createOrderParams: CreateGrowerMarketplaceOrderInput = null;
      let unsavedMarketplaceOrder: MarketplaceOrderModel = null;
      let returnedDTO: MarketplaceOrderDTO;

      let sendGrowerMarketplaceOrderCreatedMock = jest.fn(() => Promise.resolve());
      let originalSendGrowerMarketplaceOrderCreated = EmailService.prototype.sendMarketplaceOrderClosed;
      let supplyProfile: MarketplaceSupplyProfileModel = null;

      beforeAll(() => {
        UserService.getAccount = getAccountMock;
      });

      beforeEach(async () => {
        EmailService.prototype.sendMarketplaceOrderCreated = sendGrowerMarketplaceOrderCreatedMock;

        supplyProfile = await factory.create('MarketplaceSupplyProfileWithProductionLocation', {
          accountUid: dummyUser.primaryAccountId
        });
        unsavedMarketplaceOrder = await factory.build('MarketplaceOrder', {
          supplyProfileId: supplyProfile.id
        });

        createOrderParams = unsavedMarketplaceOrder.toGrowerOfferCreateInput();
      });

      afterEach(() => {
        EmailService.prototype.sendMarketplaceOrderCreated = originalSendGrowerMarketplaceOrderCreated;
      });

      afterAll(() => {
        UserService.getAccount = originalGetAccountMock;
      });

      describe('validations', () => {
        it('should throw an error if the latest MSA has not been signed', async () => {
          expect.assertions(1);
          createOrderParams.crop = Crop.SOYBEANS;
          createOrderParams.price.cashInput = {
            value: 0.25,
            currencyCode: 'USD'
          };
          createOrderParams.price.basisInput = null;
          latestAgreementSigned = false; // Simulate latest agreement not being signed
          try {
            await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
          } catch (err) {
            expect(err.message).toEqual(
              `user has not accepted the latest user agreement for state MASTER with version 1 and type SELLER.`
            );
          }
        });

        describe('Basis not supported', () => {
          let apiError: Error = null;
          beforeEach(async () => {
            await factory.create('SupportedPricing', {
              crop: createOrderParams.crop,
              defaultPricingType: CropPricingType.CASH,
              supportedPricingTypes: [CropPricingType.CASH]
            });
            createOrderParams.crop = Crop.CORN_TWOYELLOW;
            createOrderParams.price.basisInput = {
              month: FuturesMonthCode.K,
              value: 0.1,
              year: 2019
            };
            createOrderParams.price.cashInput = null;

            try {
              returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
                null,
                { input: createOrderParams },
                dummyContext
              );
            } catch (err) {
              apiError = err;
            }
          });

          it('should not return a DTO', () => {
            expect(returnedDTO).not.toBeDefined();
          });

          it("throw an error message indicating the pricing isn't supported for that crop", () => {
            expect(apiError).not.toBeNull();
            expect(apiError.message).toMatch(
              /^Cannot create marketplace order.  Basis pricing is not supported for crop/
            );
          });
        });
        describe('Cannot have Cash and Basis price', () => {
          let apiError: Error = null;
          beforeEach(async () => {
            try {
              createOrderParams.price = {
                basisInput: {
                  month: FuturesMonthCode.K,
                  value: 0.1,
                  year: 2019
                },
                cashInput: {
                  value: 1.5,
                  currencyCode: 'USD'
                }
              };
              createOrderParams.price.basisInput;
              returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
                null,
                { input: createOrderParams },
                dummyContext
              );
            } catch (err) {
              apiError = err;
            }
          });
          it('should throw an error', () => {
            expect(apiError).not.toBeNull();
            expect(apiError.message).toMatch(/^Cannot create a marketplace order with cash and basis data/);
          });

          it('should not send a confirmation email to the grower', () => {
            expect(EmailService.prototype.sendMarketplaceOrderCreated).not.toBeCalled();
          });

          it('should not return a DTO', () => {
            expect(returnedDTO).not.toBeDefined();
          });
        });
        describe('Neither Cash nor Basis price', () => {
          let apiError: Error = null;
          beforeEach(async () => {
            try {
              createOrderParams.price.basisInput = undefined;
              createOrderParams.price.cashInput = undefined;
              returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
                null,
                { input: createOrderParams },
                dummyContext
              );
            } catch (err) {
              apiError = err;
            }
          });
          it('should throw an error', () => {
            expect(apiError).not.toBeNull();
            expect(apiError.message).toMatch(/^Cannot create a marketplace order with neither cash nor basis data/);
          });

          it('should not send a confirmation email to the grower', () => {
            expect(EmailService.prototype.sendMarketplaceOrderCreated).not.toBeCalled();
          });

          it('should not return a DTO', () => {
            expect(returnedDTO).not.toBeDefined();
          });
        });

        describe('Zero cash price', () => {
          let apiError: Error;
          beforeEach(async () => {
            createOrderParams.price.cashInput = {
              value: 0,
              currencyCode: 'USD'
            };
            createOrderParams.price.basisInput = null;

            try {
              returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
                null,
                { input: createOrderParams },
                dummyContext
              );
            } catch (err) {
              apiError = err;
            }
          });
          it('should throw an error if cash price is zero', () => {
            expect(apiError).not.toBeNull();
            expect(apiError.message).toMatch(/^Cannot create a marketplace order with negative or 0 cash value/);
          });

          it('should not send a confirmation email to the grower', () => {
            expect(EmailService.prototype.sendMarketplaceOrderCreated).not.toBeCalled();
          });

          it('should not return a DTO', () => {
            expect(returnedDTO).not.toBeDefined();
          });
        });
      });

      describe('Creating an offer', () => {
        let pricingSpy: jest.SpyInstance = null;
        let orderSavedToDb: MarketplaceOrderModel = null;
        let returnedDTO: MarketplaceOrderDTO = null;

        beforeEach(() => {
          pricingSpy = jest.spyOn(PricingService, 'getPricingCapabilities');
        });

        afterEach(() => {
          pricingSpy.mockRestore();
        });

        describe('Creating basis offer', () => {
          beforeEach(async () => {
            createOrderParams.crop = Crop.CORN_TWOYELLOW;
            createOrderParams.price.basisInput = {
              month: FuturesMonthCode.K,
              value: 0.1,
              year: 2019
            };
            createOrderParams.price.cashInput = null;

            await factory.create('SupportedPricing', { crop: createOrderParams.crop });

            returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
            orderSavedToDb = await MarketplaceOrderModel.find({
              where: { uid: returnedDTO.id },
              include: [AddressModel, MarketplaceSupplyProfileModel]
            });
          });

          describe('With zero value', () => {
            beforeEach(async () => {
              createOrderParams.price.basisInput.value = 0;
              returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
                null,
                { input: createOrderParams },
                dummyContext
              );
              orderSavedToDb = await MarketplaceOrderModel.find({
                where: { uid: returnedDTO.id },
                include: [AddressModel, MarketplaceSupplyProfileModel]
              });
            });

            it('should create the marketplace order with the correct attributes', async () => {
              compareGrowerOfferInputToSavedData(createOrderParams, orderSavedToDb);
            });

            it('should return a marketplace order dto with the correct attributes', () => {
              compareGrowerOfferInputToReturnedDTO(createOrderParams, returnedDTO);
            });
          });

          it('should call the pricing service', () => {
            expect(pricingSpy).toBeCalledTimes(1);
          });

          it('should create the marketplace order with the correct attributes', async () => {
            compareGrowerOfferInputToSavedData(createOrderParams, orderSavedToDb);
          });

          it('should be associated to the correct supply profile', () => {
            expect(orderSavedToDb.supplyProfileId).toEqual(supplyProfile.id);
          });

          it('should return a marketplace order dto with the correct attributes', () => {
            compareGrowerOfferInputToReturnedDTO(createOrderParams, returnedDTO);
          });
        });

        describe('Creating Cash Offer', () => {
          beforeEach(async () => {
            createOrderParams.crop = Crop.SOYBEANS;
            createOrderParams.price.cashInput = {
              value: 0.25,
              currencyCode: 'USD'
            };
            createOrderParams.price.basisInput = null;

            returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
            orderSavedToDb = await MarketplaceOrderModel.find({
              where: { uid: returnedDTO.id },
              include: [AddressModel, MarketplaceSupplyProfileModel]
            });
          });

          it('should not call the pricing service', () => {
            expect(pricingSpy).toBeCalledTimes(0);
          });

          it('should create the marketplace order with the correct attributes', async () => {
            compareGrowerOfferInputToSavedData(createOrderParams, orderSavedToDb);
          });

          it('should be associated to the correct supply profile', () => {
            expect(orderSavedToDb.supplyProfileId).toEqual(supplyProfile.id);
          });

          it('should return a marketplace order dto with the correct attributes', () => {
            compareGrowerOfferInputToReturnedDTO(createOrderParams, returnedDTO);
          });
        });

        describe('Creating Cash Offer with originating demand order', () => {
          let originatingOrder: DemandOrderModel;
          beforeEach(async () => {
            originatingOrder = await factory.create('DemandOrderWithCashPrice');
            createOrderParams.originatingDemandOrderId = originatingOrder.uid;
            createOrderParams.crop = Crop.SOYBEANS;
            createOrderParams.price.cashInput = {
              value: 0.25,
              currencyCode: 'USD'
            };
            createOrderParams.price.basisInput = null;

            returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
            orderSavedToDb = await MarketplaceOrderModel.find({
              where: { uid: returnedDTO.id },
              include: [AddressModel, MarketplaceSupplyProfileModel]
            });
          });

          it('should not call the pricing service', () => {
            expect(pricingSpy).toBeCalledTimes(0);
          });

          it('should create the marketplace order with the correct attributes', async () => {
            compareGrowerOfferInputToSavedData(createOrderParams, orderSavedToDb);
          });

          it('should be associated to the correct supply profile', () => {
            expect(orderSavedToDb.supplyProfileId).toEqual(supplyProfile.id);
          });

          it('should return a marketplace order dto with the correct attributes', () => {
            compareGrowerOfferInputToReturnedDTO(createOrderParams, returnedDTO);
          });

          it('should have the correct INTERNAL originating demand order ID on the DTO', () => {
            expect(returnedDTO.originatingDemandOrderId).toEqual(originatingOrder.id);
          });

          it('should have the correct INTERNAL originating demand order ID on the model', () => {
            expect(returnedDTO.originatingDemandOrderId).toEqual(originatingOrder.id);
          });
        });

        describe('Create an anonymous order', () => {
          beforeEach(async () => {
            createOrderParams.isAnonymous = true;
            returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
            orderSavedToDb = await MarketplaceOrderModel.find({
              where: { uid: returnedDTO.id },
              include: [AddressModel, MarketplaceSupplyProfileModel]
            });
          });

          it('should not have an accountName', () => {
            expect(orderSavedToDb.accountName).toEqual(null);
          });
        });
        describe('Create a public order', () => {
          beforeEach(async () => {
            createOrderParams.isAnonymous = false;
            returnedDTO = await MarketplaceOrderMutations.createGrowerMarketplaceOrder(
              null,
              { input: createOrderParams },
              dummyContext
            );
            orderSavedToDb = await MarketplaceOrderModel.find({
              where: { uid: returnedDTO.id },
              include: [AddressModel, MarketplaceSupplyProfileModel]
            });
          });

          it('should not have an accountName', () => {
            expect(orderSavedToDb.accountName).toBe(`${globalTestAccountName}`);
          });
        });
      });
    });

    describe('closeAndCreateNewGrowerMarketplaceOrder', () => {
      let createOrderParams: CreateGrowerMarketplaceOrderInput = null;
      let unsavedMarketplaceOrder: MarketplaceOrderModel = null;

      let originalSendGrowerMarketplaceOrderCreated = EmailService.prototype.sendMarketplaceOrderClosed;
      let sendGrowerMarketplaceOrderCreatedMock = jest.fn(() => Promise.resolve());
      let supplyProfile: MarketplaceSupplyProfileModel = null;

      beforeEach(async () => {
        EmailService.prototype.sendMarketplaceOrderCreated = sendGrowerMarketplaceOrderCreatedMock;

        supplyProfile = await factory.create('MarketplaceSupplyProfileWithProductionLocation');
        unsavedMarketplaceOrder = await factory.build('MarketplaceOrder', {
          supplyProfileId: supplyProfile.id
        });

        createOrderParams = unsavedMarketplaceOrder.toGrowerOfferCreateInput();
        createOrderParams.crop = Crop.CORN_TWOYELLOW;
        createOrderParams.price.basisInput = {
          month: FuturesMonthCode.K,
          value: 0.1,
          year: 2019
        };
        createOrderParams.price.cashInput = null;

        await factory.create('SupportedPricing', { crop: createOrderParams.crop });
      });

      afterEach(() => {
        EmailService.prototype.sendMarketplaceOrderCreated = originalSendGrowerMarketplaceOrderCreated;
      });

      describe('Validating operation', () => {
        let orderToCloseModel: MarketplaceOrderModel = null;
        let originalDALcloseAndCreateNewGrowerMarketplaceOrder =
          MarketplaceOrderDAL.closeAndCreateNewGrowerMarketplaceOrder;
        let caughtError: Error;
        let pricingSpy: jest.SpyInstance = null;
        const mockDALcloseAndCreateNewGrowerMarketplaceOrder = jest.fn();

        beforeEach(async () => {
          caughtError = undefined;
          mockDALcloseAndCreateNewGrowerMarketplaceOrder.mockClear();
          pricingSpy = jest.spyOn(PricingService, 'getPricingCapabilities');
        });

        afterEach(() => {
          pricingSpy.mockRestore();
        });

        describe('Editing a closed offer', () => {
          beforeEach(async () => {
            orderToCloseModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.CLOSED });
            try {
              await MarketplaceOrderMutations.closeAndCreateNewGrowerMarketplaceOrder(
                null,
                { input: createOrderParams, closeGrowerMarketplaceOrderId: orderToCloseModel.uid },
                dummyContext
              );
            } catch (e) {
              caughtError = e;
            }
          });

          afterEach(() => {
            MarketplaceOrderDAL.closeAndCreateNewGrowerMarketplaceOrder = originalDALcloseAndCreateNewGrowerMarketplaceOrder;
            pricingSpy.mockRestore();
          });

          it('should not update the database', () => {
            expect(mockDALcloseAndCreateNewGrowerMarketplaceOrder.mock.calls.length).toEqual(0);
          });

          it('should not send any emails', () => {
            expect(sendGrowerMarketplaceOrderCreatedMock.mock.calls.length).toEqual(0);
          });

          it('should throw an error', () => {
            expect(caughtError).not.toBe(undefined);
          });
        });

        describe('Editing an accepted offer', () => {
          beforeEach(async () => {
            orderToCloseModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.ACCEPTED });
            try {
              await MarketplaceOrderMutations.closeAndCreateNewGrowerMarketplaceOrder(
                null,
                { input: createOrderParams, closeGrowerMarketplaceOrderId: orderToCloseModel.uid },
                dummyContext
              );
            } catch (e) {
              caughtError = e;
            }
          });

          afterEach(() => {
            MarketplaceOrderDAL.closeAndCreateNewGrowerMarketplaceOrder = originalDALcloseAndCreateNewGrowerMarketplaceOrder;
            pricingSpy.mockRestore();
          });

          it('should not update the database', () => {
            expect(mockDALcloseAndCreateNewGrowerMarketplaceOrder.mock.calls.length).toEqual(0);
          });

          it('should not send any emails', () => {
            expect(sendGrowerMarketplaceOrderCreatedMock.mock.calls.length).toEqual(0);
          });

          it('should throw an error', () => {
            expect(caughtError).not.toBe(undefined);
          });
        });

        describe('Basis not supported', () => {
          beforeEach(async () => {
            await factory.create('SupportedPricing', {
              crop: createOrderParams.crop,
              defaultPricingType: CropPricingType.CASH,
              supportedPricingTypes: [CropPricingType.CASH]
            });
            createOrderParams.crop = Crop.CORN_TWOYELLOW;
            createOrderParams.price.basisInput = {
              month: FuturesMonthCode.K,
              value: 0.1,
              year: 2019
            };
            createOrderParams.price.cashInput = null;

            orderToCloseModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.OPEN });
            try {
              await MarketplaceOrderMutations.closeAndCreateNewGrowerMarketplaceOrder(
                null,
                { input: createOrderParams, closeGrowerMarketplaceOrderId: orderToCloseModel.uid },
                dummyContext
              );
            } catch (err) {
              caughtError = err;
            }
          });

          it('should not update the database', () => {
            expect(mockDALcloseAndCreateNewGrowerMarketplaceOrder.mock.calls.length).toEqual(0);
          });

          it('should not send any emails', () => {
            expect(sendGrowerMarketplaceOrderCreatedMock.mock.calls.length).toEqual(0);
          });

          it('should throw an error', () => {
            expect(caughtError).not.toBe(undefined);
          });
        });

        describe('Neither Cash nor basis price', () => {
          beforeEach(async () => {
            createOrderParams.crop = Crop.CORN_TWOYELLOW;
            createOrderParams.price = {};

            orderToCloseModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.OPEN });
            try {
              await MarketplaceOrderMutations.closeAndCreateNewGrowerMarketplaceOrder(
                null,
                { input: createOrderParams, closeGrowerMarketplaceOrderId: orderToCloseModel.uid },
                dummyContext
              );
            } catch (err) {
              caughtError = err;
            }
          });

          it('should not update the database', () => {
            expect(mockDALcloseAndCreateNewGrowerMarketplaceOrder.mock.calls.length).toEqual(0);
          });

          it('should not send any emails', () => {
            expect(sendGrowerMarketplaceOrderCreatedMock.mock.calls.length).toEqual(0);
          });

          it('should throw an error', () => {
            expect(caughtError).not.toBe(undefined);
            expect(caughtError.message).toMatch(/^Cannot create a marketplace order with neither cash nor basis data/);
          });
        });
      });

      describe('Editing Open Grower Offer', () => {
        let orderToCloseModel: MarketplaceOrderModel = null;
        let closedOrderModel: MarketplaceOrderModel = null;
        let createdOrderModel: MarketplaceOrderModel = null;
        let pricingSpy: jest.SpyInstance = null;
        let returnedDTO: MarketplaceOrderDTO = null;

        beforeEach(async () => {
          pricingSpy = jest.spyOn(PricingService, 'getPricingCapabilities');
          orderToCloseModel = await factory.create('MarketplaceOrder', { status: MarketOrderStatus.OPEN });
          returnedDTO = await MarketplaceOrderMutations.closeAndCreateNewGrowerMarketplaceOrder(
            null,
            { input: createOrderParams, closeGrowerMarketplaceOrderId: orderToCloseModel.uid },
            dummyContext
          );
          createdOrderModel = await MarketplaceOrderModel.find({
            where: { uid: returnedDTO.id },
            include: [AddressModel, MarketplaceSupplyProfileModel]
          });
          closedOrderModel = await MarketplaceOrderModel.find({
            where: { uid: orderToCloseModel.uid },
            include: [AddressModel, MarketplaceSupplyProfileModel]
          });
        });

        afterEach(() => {
          pricingSpy.mockRestore();
        });

        it('should close the specified order in the database', () => {
          expect(closedOrderModel.status).toEqual(MarketOrderStatus.CLOSED);
        });

        it('should create the marketplace order with the correct attributes', async () => {
          compareGrowerOfferInputToSavedData(createOrderParams, createdOrderModel);
        });

        it('should be associated to the correct supply profile', () => {
          expect(createdOrderModel.supplyProfileId).toEqual(supplyProfile.id);
        });

        it('should return a marketplace order dto with the correct attributes', () => {
          compareGrowerOfferInputToReturnedDTO(createOrderParams, returnedDTO);
        });
      });
    });

    describe('createHedgeOrder', () => {
      const originalMarketplaceOrderServiceCreateHedgeOrder = MarketplaceOrderService.createHedgeOrder;
      let mockMarketplaceOrderServiceCreateHedgeOrder = jest.fn();

      let createHedgeOrderInput: CreateHedgeOrderInput = null;
      let unsavedMarketplaceOrder: MarketplaceOrderModel = null;

      beforeEach(async () => {
        unsavedMarketplaceOrder = await factory.build('MarketplaceOrderWithFloatingBasis');

        createHedgeOrderInput = unsavedMarketplaceOrder.toCreateHedgeOrderInput();

        MarketplaceOrderService.createHedgeOrder = mockMarketplaceOrderServiceCreateHedgeOrder;
      });

      afterEach(() => {
        mockMarketplaceOrderServiceCreateHedgeOrder.mockReset();
      });

      afterAll(() => {
        MarketplaceOrderService.createHedgeOrder = originalMarketplaceOrderServiceCreateHedgeOrder;
      });

      it('should call MarketplaceOrderService.createHedgeOrder', async () => {
        expect.assertions(3);

        await MarketplaceOrderMutations.createHedgeOrder(
          null,
          {
            input: createHedgeOrderInput
          },
          dummyContext
        );

        expect(mockMarketplaceOrderServiceCreateHedgeOrder).toBeCalledTimes(1);
        expect(mockMarketplaceOrderServiceCreateHedgeOrder.mock.calls[0][0]).toEqual(createHedgeOrderInput);
        expect(mockMarketplaceOrderServiceCreateHedgeOrder.mock.calls[0][1]).toEqual(dummyContext);
      });
    });

    describe('fillHedgeOrder', () => {
      const originalMarketplaceOrderServiceFillHedgeOrder = MarketplaceOrderService.fillHedgeOrder;
      let mockMarketplaceOrderServiceFillHedgeOrder = jest.fn();
      const defaultFee: Currency = {
        code: 'USD',
        value: 0.03
      };

      beforeEach(async () => {
        MarketplaceOrderService.fillHedgeOrder = mockMarketplaceOrderServiceFillHedgeOrder;
      });

      afterEach(() => {
        mockMarketplaceOrderServiceFillHedgeOrder.mockReset();
      });

      afterAll(() => {
        MarketplaceOrderService.fillHedgeOrder = originalMarketplaceOrderServiceFillHedgeOrder;
      });

      it('should call MarketplaceOrderService.fillHedgeOrder', async () => {
        expect.assertions(3);

        const input = {
          filledQuantity: 500,
          hedgeFee: defaultFee,
          marketplaceOrderId: 'valid'
        };

        await MarketplaceOrderMutations.fillHedgeOrder(null, { input }, dummyContext);

        expect(mockMarketplaceOrderServiceFillHedgeOrder).toBeCalledTimes(1);
        expect(mockMarketplaceOrderServiceFillHedgeOrder.mock.calls[0][0]).toEqual(input);
        expect(mockMarketplaceOrderServiceFillHedgeOrder.mock.calls[0][1]).toEqual(dummyContext);
      });
    });
  });

  describe('MarketplaceOrderDataLoaders', () => {
    let marketplaceOrderDataLoaders: any = null;
    beforeEach(() => {
      marketplaceOrderDataLoaders = createMarketplaceOrderDataLoaders();
    });

    describe('marketplaceOrdersByUid', () => {
      let foundOrderDTOs: MarketplaceOrderDTO[] = null;
      let orderModels: MarketplaceOrderModel[] = null;
      let idsToRetrieve: string[] = null;
      let idsNotToRetrieve: string[] = null;
      beforeEach(async () => {
        orderModels = await factory.createMany('MarketplaceOrder', 4);
        idsToRetrieve = orderModels
          .slice(0, 2)
          .map((item: MarketplaceOrderModel) => item.uid)
          .sort();
        idsNotToRetrieve = orderModels
          .slice(2, 4)
          .map((item: MarketplaceOrderModel) => item.uid)
          .sort();
        foundOrderDTOs = await marketplaceOrderDataLoaders.marketplaceOrdersByUid.loadMany(idsToRetrieve);
      });

      it('should return orders corresponding to the UIDs passed', () => {
        expect(idsToRetrieve).toEqual(foundOrderDTOs.map((item: MarketplaceOrderDTO) => item.id));
      });

      it("should not contain the ids we didn't request", () => {
        expect(foundOrderDTOs.map((item: MarketplaceOrderDTO) => item.id)).not.toContain(idsNotToRetrieve);
      });
    });

    describe('marketplaceOrdersById', () => {
      let foundOrderDTOs: MarketplaceOrderDTO[] = null;
      let orderModels: MarketplaceOrderModel[] = null;
      let idsToRetrieve: string[] = null;
      let idsNotToRetrieve: string[] = null;
      beforeEach(async () => {
        orderModels = await factory.createMany('MarketplaceOrder', 4);
        idsToRetrieve = orderModels
          .slice(0, 2)
          .map((item: MarketplaceOrderModel) => item.id)
          .sort();
        idsNotToRetrieve = orderModels
          .slice(2, 4)
          .map((item: MarketplaceOrderModel) => item.id)
          .sort();
        foundOrderDTOs = await marketplaceOrderDataLoaders.marketplaceOrdersById.loadMany(idsToRetrieve);
      });

      it('should return orders corresponding to the UIDs passed', () => {
        expect(idsToRetrieve).toEqual(
          foundOrderDTOs.map((item: MarketplaceOrderDTO) => item.marketplaceOrderDatabaseId)
        );
      });

      it("should not contain the ids we didn't request", () => {
        expect(foundOrderDTOs.map((item: MarketplaceOrderDTO) => item.marketplaceOrderDatabaseId)).not.toContain(
          idsNotToRetrieve
        );
      });
    });
  });

  describe('MarketplaceOrderTypeResolvers', () => {
    describe('GrowerOfferForDemand.marketplaceOrder', () => {
      let returnedOrder: MarketplaceOrderDTO = null;
      let savedMarketplaceOrder: MarketplaceOrderModel = null;
      beforeEach(async () => {
        savedMarketplaceOrder = await factory.create('MarketplaceOrder');
        returnedOrder = await MarketplaceOrderTypeResolvers.GrowerOfferForDemand.marketplaceOrder(
          {
            shippingProvider: ShippingProvider.SELLER,
            shippingDistance: 100,
            maxQuantity: 100,
            minQuantity: 100,
            id: savedMarketplaceOrder.uid,
            marketplaceOrder: new MarketplaceOrderDTO(savedMarketplaceOrder, null)
          },
          null,
          createDataLoaderContextForTest()
        );
      });

      it('should return a marketplace order', () => {
        expect(new MarketplaceOrderDTO(savedMarketplaceOrder, null)).toEqual(returnedOrder);
      });
    });

    describe('MarketplaceOrder.seller', () => {
      let marketplaceOrder: MarketplaceOrderModel = null;
      let returnedSupplyProfile: MarketplaceSupplyProfileDTO = null;

      beforeEach(async () => {
        marketplaceOrder = await factory.create('MarketplaceOrder');
        returnedSupplyProfile = (await MarketplaceOrderTypeResolvers.MarketplaceOrder.seller(
          new MarketplaceOrderDTO(marketplaceOrder, null),
          null,
          createDataLoaderContextForTest()
        )) as MarketplaceSupplyProfileDTO;
      });

      it('should return a supply profile', () => {
        expect(new MarketplaceSupplyProfileDTO(marketplaceOrder.supplyProfile)).toEqual(returnedSupplyProfile);
      });
    });

    describe('MarketplaceOrder.address', () => {
      let marketplaceOrderModel: MarketplaceOrderModel = null;
      beforeEach(async () => {
        marketplaceOrderModel = await factory.create('MarketplaceOrder');
      });
      it('should return the address associated to the marketplace order', async () => {
        let returnedAddressDTO: AddressDTO = (await MarketplaceOrderTypeResolvers.MarketplaceOrder.address(
          new MarketplaceOrderDTO(marketplaceOrderModel, null),
          null,
          createDataLoaderContextForTest()
        )) as AddressDTO;

        const marketplaceOrderModelAddress = marketplaceOrderModel.address;
        const { id: dtoId, internalId: dtoDatabaseId, ...restOfDTOAttributes } = returnedAddressDTO;
        const { id: addressModelId, uid: modelUid, ...restOfAddressModelAttributes } = marketplaceOrderModekAddress.get(
          { plain: true }
        );
        expect(modelUid).toEqual(dtoId);
        expect(addressModelId).toEqual(dtoDatabaseId);
        expect(restOfAddressModelAttributes).toMatchObject(restOfDTOAttributes);
      });

      it('should return null when there is no address associated to the order', async () => {
        marketplaceOrderModel.addressId = null;
        let returnedAddressDTO: AddressDTO = (await MarketplaceOrderTypeResolvers.MarketplaceOrder.address(
          new MarketplaceOrderDTO(marketplaceOrderModel, null),
          null,
          createDataLoaderContextForTest()
        )) as AddressDTO;
        expect(returnedAddressDTO).toBeNull();
      });
    });

    describe('MarketplaceOrder.acceptance', () => {
      let marketplaceOrderModelWithoutAcceptance: MarketplaceOrderModel = null;
      let marketplaceOrderModelWithAcceptance: MarketplaceOrderModel = null;
      let acceptance: MarketplaceAcceptanceModel = null;
      beforeEach(async () => {
        marketplaceOrderModelWithoutAcceptance = await factory.create('MarketplaceOrder');
        acceptance = await factory.create('MarketplaceAcceptance');
        marketplaceOrderModelWithAcceptance = acceptance.marketplaceOrder;
      });

      it('should return the acceptance associated with the marketplace order passed in', async () => {
        let returnedAcceptanceDTO: BidAcceptanceDTO = await MarketplaceOrderTypeResolvers.MarketplaceOrder.acceptance(
          new MarketplaceOrderDTO(marketplaceOrderModelWithAcceptance, null),
          null,
          createDataLoaderContextForTest()
        );
        expect(returnedAcceptanceDTO.acceptedAt).toEqual(acceptance.createdAt.toISOString());
        expect(returnedAcceptanceDTO.buyerPrice).toEqual({
          code: acceptance.currencyCode,
          value: acceptance.buyerPrice
        });
        expect(returnedAcceptanceDTO.crop).toEqual(acceptance.crop);
        expect(returnedAcceptanceDTO.cropQuantity).toEqual(acceptance.cropQuantity);
        expect(returnedAcceptanceDTO.cropUnit).toEqual(acceptance.cropUnit);
        expect(returnedAcceptanceDTO.id).toEqual(acceptance.uid);
        expect(returnedAcceptanceDTO.referenceId).toEqual(acceptance.referenceId);
        expect(returnedAcceptanceDTO.sellerPrice).toEqual({
          code: acceptance.currencyCode,
          value: acceptance.sellerPrice
        });
        expect(returnedAcceptanceDTO.shippingDistance).toEqual(acceptance.shippingDistance);
        expect(returnedAcceptanceDTO.shippingProvider).toEqual(acceptance.shippingProvider);
      });

      it('should return null if the marketplace order has no acceptance', async () => {
        let returnedAcceptanceDTO: BidAcceptanceDTO = await MarketplaceOrderTypeResolvers.MarketplaceOrder.acceptance(
          new MarketplaceOrderDTO(marketplaceOrderModelWithoutAcceptance, null),
          null,
          createDataLoaderContextForTest()
        );

        expect(returnedAcceptanceDTO).toBeNull();
      });
    });

    describe('MarketplaceOrder.originatingDemandOrder', () => {
      let marketplaceOrderModelWithoutOriginatingDemandOrder: MarketplaceOrderModel = null;
      let marketplaceOrderModelWithOriginatingDemandOrder: MarketplaceOrderModel = null;
      let originatingDemandOrder: DemandOrderModel = null;
      beforeEach(async () => {
        marketplaceOrderModelWithoutOriginatingDemandOrder = await factory.create('MarketplaceOrder');
        marketplaceOrderModelWithOriginatingDemandOrder = await factory.create('MarketplaceOrder');
        originatingDemandOrder = await factory.create('DemandOrder');
        marketplaceOrderModelWithOriginatingDemandOrder.originatingDemandOrderId = originatingDemandOrder.id;
      });

      it('should return the DemandOrder associated with the marketplace order passed in', async () => {
        let returnedDemandOrderDTO: DemandOrderDTO = await MarketplaceOrderTypeResolvers.MarketplaceOrder.originatingDemandOrder(
          new MarketplaceOrderDTO(marketplaceOrderModelWithOriginatingDemandOrder, null),
          null,
          createDataLoaderContextForTest()
        );
        expect(returnedDemandOrderDTO.id).toEqual(originatingDemandOrder.uid);
        expect(returnedDemandOrderDTO.isPremium).toEqual(originatingDemandOrder.isPremium);
        expect(returnedDemandOrderDTO.ownedBy).toEqual(originatingDemandOrder.ownedBy);
        expect(returnedDemandOrderDTO.price.code).toEqual(originatingDemandOrder.currencyCode);
        expect(returnedDemandOrderDTO.price.value).toEqual(originatingDemandOrder.price);
        expect(returnedDemandOrderDTO.crop).toEqual(originatingDemandOrder.crop);
        expect(returnedDemandOrderDTO.cropQuantity).toEqual(originatingDemandOrder.cropQuantity);
        expect(returnedDemandOrderDTO.buyerType).toEqual(originatingDemandOrder.buyerType);
      });

      it('should return null if the marketplace order has no originatingDemandOrderId', async () => {
        let returnedDemandOrderDTO: DemandOrderDTO = await MarketplaceOrderTypeResolvers.MarketplaceOrder.originatingDemandOrder(
          new MarketplaceOrderDTO(marketplaceOrderModelWithoutOriginatingDemandOrder, null),
          null,
          createDataLoaderContextForTest()
        );

        expect(returnedDemandOrderDTO).toBeNull();
      });
    });
  });
});

function compareGrowerOfferInputToReturnedDTO(input: CreateGrowerMarketplaceOrderInput, dto: MarketplaceOrderDTO) {
  if (input.price.cashInput) {
    const price: Currency = dto.price as Currency;
    expect(price.value).toEqual(dto.price.value);
    expect(input.price.cashInput.currencyCode).toEqual(price.code);
  } else {
    const price: BasisPrice = dto.price as BasisPrice;
    expect(input.price.basisInput.month).toEqual(price.monthCode);
    expect(input.price.basisInput.year).toEqual(price.year);
    expect(input.price.basisInput.value).toEqual(price.value);
  }

  expect(input.crop).toEqual(dto.crop);
  if (input.cropQuality) {
    expect(input.cropQuality).toEqual(dto.cropQuality);
  }
  expect(input.cropQuantity).toEqual(dto.cropQuantity);
  expect(input.cropQuantityUnit).toEqual(dto.cropQuantityUnit);
  expect(input.deliveryWindowEndAt).toEqual(dto.deliveryWindowEndAt);
  expect(input.deliveryWindowStartAt).toEqual(dto.deliveryWindowStartAt);
  expect(input.expiresAt).toEqual(dto.expiresAt);
  expect(input.maxRadiusMiles).toEqual(dto.maxRadiusMiles);
  expect(input.notes).toEqual(dto.notes);
  expect(dto.referenceId).toMatch(/^MO/);
  expect(dto.status).toEqual(MarketOrderStatus.WORKING);
  expect(dto.accountName).toEqual(null);
}

function compareGrowerOfferInputToSavedData(input: CreateGrowerMarketplaceOrderInput, model: MarketplaceOrderModel) {
  if (input.price.cashInput) {
    expect(input.price.cashInput.value).toEqual(model.cashPrice);
    expect(input.price.cashInput.currencyCode).toEqual(model.cashPriceCurrencyCode);
  } else {
    expect(input.price.basisInput.month).toEqual(model.basisMonthCode);
    expect(input.price.basisInput.year).toEqual(model.basisYear);
    expect(input.price.basisInput.value).toEqual(model.basisValue);
  }

  expect(input.crop).toEqual(model.crop);
  expect(input.cropQuality).toEqual(model.cropQuality);
  expect(input.cropQuantity).toEqual(model.cropQuantity);
  expect(input.cropQuantityUnit).toEqual(model.cropQuantityUnit);
  expect(input.deliveryWindowEndAt).toEqual(model.deliveryWindowEndAt.toISOString());
  expect(input.deliveryWindowStartAt).toEqual(model.deliveryWindowStartAt.toISOString());  expect(input.expiresAt).toEqual(model.expiresAt.toISOString());
  expect(input.maxRadiusMiles).toEqual(model.maxRadiusMiles);
  expect(input.notes).toEqual(model.notes);
  expect(model.address).toMatchObject(input.pickupAddress);
  expect(model.referenceId).toMatch(/^MO/);
  expect(model.status).toEqual(MarketOrderStatus.WORKING);
  expect(model.accountName).toEqual(null);
}
