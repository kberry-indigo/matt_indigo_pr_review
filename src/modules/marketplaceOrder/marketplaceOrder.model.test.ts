import MarketplaceOrderModel from './marketplaceOrder.model';
import { databaseTestFramework } from '../../../test/framework/database';
import { FuturesMonthCode, MarketOrderStatus } from '@indigo-ag/schema';
import { factory } from 'factory-girl';

describe('MarketplaceOrderModel', () => {
  beforeAll(async () => {
    return databaseTestFramework.acquireDatabase();
  });

  afterAll(async () => {
    factory.created.clear();
    return databaseTestFramework.releaseDatabase();
  });

  beforeEach(async () => {
    return databaseTestFramework.clearDatabase();
  });

  describe('isBasis', () => {
    it('should return true for a basis model with positive basisValue', async () => {
      const order = new MarketplaceOrderModel({
        basisValue: 0.1,
        basisYear: 2019,
        basisMonthCode: FuturesMonthCode.F
      });
      expect(order.isBasisOffer()).toBeTruthy();
    });

    it('should return true for a basis model with negative basisValue', async () => {
      const order = new MarketplaceOrderModel({
        basisValue: -1.3,
        basisYear: 2019,
        basisMonthCode: FuturesMonthCode.F
      });
      expect(order.isBasisOffer()).toBeTruthy();
    });

    it('should return true for a basis model with basisValue of 0', async () => {
      const order = new MarketplaceOrderModel({
        basisValue: 0,
        basisYear: 2019,
        basisMonthCode: FuturesMonthCode.F
      });
      expect(order.isBasisOffer()).toBeTruthy();
    });

    it('should return false for a cash model', async () => {
      const order = new MarketplaceOrderModel({
        cashPrice: 0.75,
        cashPriceCurrencyCode: 'USD'
      });
      expect(order.isBasisOffer()).toBeFalsy();
    });
  });

  describe('isExpired', () => {
    let marketplaceOrderModel: MarketplaceOrderModel = null;
    beforeEach(async () => {
      marketplaceOrderModel = new MarketplaceOrderModel();
    });

    it('should return true if the offer expiration is before today', () => {
      const today = new Date(2019, 10, 15);
      marketplaceOrderModel.expiresAt = new Date(2019, 10, 14);
      expect(marketplaceOrderModel.isExpired(today)).toEqual(true);
    });

    it('should return false if the offer expiration is after today', () => {
      const today = new Date(2019, 10, 15);
      marketplaceOrderModel.expiresAt = new Date(2019, 10, 16);
      expect(marketplaceOrderModel.isExpired(today)).toEqual(false);
    });
  });

  describe('isOpen', () => {
    it('should return true for a marketplaceOrder with status OPEN', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.OPEN
      });
      expect(order.isOpen()).toEqual(true);
    });

    it('should return true for a marketplaceOrder with status PENDING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.PENDING
      });
      expect(order.isOpen()).toEqual(true);
    });

    it('should return true for a marketplaceOrder with status WORKING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.WORKING
      });
      expect(order.isOpen()).toEqual(true);
    });

    it('should return false for a marketplaceOrder with status CLOSED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.CLOSED
      });
      expect(order.isOpen()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status FILLED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.FILLED
      });
      expect(order.isOpen()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status ACCEPTED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.ACCEPTED
      });
      expect(order.isOpen()).toEqual(false);
    });
  });

  describe('isAccepted', () => {
    it('should return false for a marketplaceOrder with status OPEN', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.OPEN
      });
      expect(order.isAccepted()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status PENDING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.PENDING
      });
      expect(order.isAccepted()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status WORKING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.WORKING
      });
      expect(order.isAccepted()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status CLOSED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.CLOSED
      });
      expect(order.isAccepted()).toEqual(false);
    });

    it('should return true for a marketplaceOrder with status FILLED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.FILLED
      });
      expect(order.isAccepted()).toEqual(true);
    });

    it('should return true for a marketplaceOrder with status ACCEPTED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.ACCEPTED
      });
      expect(order.isAccepted()).toEqual(true);
    });
  });

  describe('isClosed', () => {
    it('should return false for a marketplaceOrder with status OPEN', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.OPEN
      });
      expect(order.isClosed()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status PENDING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.PENDING
      });
      expect(order.isClosed()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status WORKING', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.WORKING
      });
      expect(order.isClosed()).toEqual(false);
    });

    it('should return true for a marketplaceOrder with status CLOSED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.CLOSED
      });
      expect(order.isClosed()).toEqual(true);
    });

    it('should return false for a marketplaceOrder with status FILLED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.FILLED
      });
      expect(order.isClosed()).toEqual(false);
    });

    it('should return false for a marketplaceOrder with status ACCEPTED', async () => {
      const order = new MarketplaceOrderModel({
        status: MarketOrderStatus.ACCEPTED
      });
      expect(order.isClosed()).toEqual(false);
    });
  });
});
