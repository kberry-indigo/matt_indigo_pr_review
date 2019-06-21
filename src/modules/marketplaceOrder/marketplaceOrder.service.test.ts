import {
  BasisPrice,
  CreateGrowerMarketplaceOrderInput,
  Crop,
  CropPricingType,
  Currency,
  FillHedgeOrderInput,
  FuturesMonthCode,
  MarketOrderStatus,
  OrderFeePayor,
  OrderFeeType
} from '@indigo-ag/schema';
import { Context, User } from '@indigo-ag/server';
import { factory } from 'factory-girl';
import * as moment from 'moment';
import { Transaction } from 'sequelize';
import { IFindOptions } from 'sequelize-typescript';
import { generateDummyContext, NON_GMA_USER } from '../../../test/fixtures/context.fixture';
import { databaseTestFramework } from '../../../test/framework/database';
import '../../../test/helpers/marketplaceOrder.test.helper';
import { OrderFeeDAL } from '../orderFees';
import { PricingService, SupportedPricingModel } from '../pricing';
import { PricingCapabilitiesDTO } from '../pricing/pricing.dto';
import { GMAService } from '../supply/gma.service';
import { HedgeOfferUpdatableAttributes, MarketplaceOrderDAL } from './marketplaceOrder.dal';
import { MarketplaceOrderDTO } from './marketplaceOrder.dto';
import MarketplaceOrderModel from './marketplaceOrder.model';
import { MarketplaceOrderNotificationService } from './marketplaceOrder.notification.service';
import {
  CreateInputPricingDetails,
  MarketplaceOrderOperationErrorCode,
  MarketplaceOrderService
} from './marketplaceOrder.service';
import BasisDetailsModel from '../pricing/basis_details.model';
import CropCalendarModel from '../pricing/cropCalendar.model';

const dummyUser: User = NON_GMA_USER;
const dummyContext: Context = generateDummyContext(dummyUser);

describe('MarketplaceOrderService', () => {
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

  // TODO: add tests for: createHedgeOrder

  describe('fillHedgeOrder', () => {
    const originalMarketplaceOrderDalUpdateHedgeOrder = MarketplaceOrderDAL.updateHedgeOrder;
    const originalCreateOrderFee = OrderFeeDAL.createOrderFee;
    const originalMarketplaceOrderDalFindByUid = MarketplaceOrderDAL.findByUid;
    const originalPricingServiceGetPricingCapabilities = PricingService.getPricingCapabilities;
    const originalSendFillHedgeOrderNotification = MarketplaceOrderNotificationService.sendFillHedgeOrderNotification;

    let mockMarketplaceOrderDalUpdateHedgeOrder = jest.fn();
    let mockCreateOrderFee = jest.fn();
    let mockMarketplaceOrderDalFindByUid = jest.fn();
    let mockPricingServiceGetPricingCapabilities = jest.fn();
    let mockSendFillHedgeOrderNotification = jest.fn();

    let otcMarketplaceOrder: MarketplaceOrderModel = null;
    let filledHedgeOrder: MarketplaceOrderModel = null;
    let closedHedgeOrder: MarketplaceOrderModel = null;
    let validHedgeOrder: MarketplaceOrderModel = null;
    const cropQuantity: number = 1000;

    const crops = [Crop.SOYBEANS];
    const pricingCapabilityResultsMap = crops.map((crop: Crop) => {
      new PricingCapabilitiesDTO(crop, {}, {}, {});
    });
    const defaultFee: Currency = {
      code: 'USD',
      value: 0.03
    };

    beforeEach(async () => {
      otcMarketplaceOrder = await factory.build('MarketplaceOrder');
      filledHedgeOrder = await factory.build('MarketplaceOrderWithFloatingBasis', {
        status: MarketOrderStatus.FILLED
      });
      closedHedgeOrder = await factory.build('MarketplaceOrderWithFloatingBasis', {
        status: MarketOrderStatus.CLOSED
      });
      validHedgeOrder = await factory.build('MarketplaceOrderWithFloatingBasis', {
        cropQuantity: cropQuantity,
        createdAt: moment('2019-05-02'),
        status: MarketOrderStatus.WORKING,
        updatedAt: moment()
      });

      mockMarketplaceOrderDalFindByUid = jest.fn((uid: string, options?: IFindOptions<MarketplaceOrderModel>) => {
        if (uid === 'otc') {
          return otcMarketplaceOrder;
        } else if (uid === 'filled') {
          return filledHedgeOrder;
        } else if (uid === 'closed') {
          return closedHedgeOrder;
        } else if (uid === 'valid') {
          return validHedgeOrder;
        } else {
          return null;
        }
      });

      mockMarketplaceOrderDalUpdateHedgeOrder = jest.fn((id: string, attrs: HedgeOfferUpdatableAttributes) => {
        let updatedMarketplaceOrder = new MarketplaceOrderModel();
        updatedMarketplaceOrder.id = validHedgeOrder.id;
        updatedMarketplaceOrder.uid = validHedgeOrder.uid;
        updatedMarketplaceOrder.referenceId = validHedgeOrder.referenceId;
        updatedMarketplaceOrder.supplyProfileId = validHedgeOrder.supplyProfileId;
        updatedMarketplaceOrder.product = validHedgeOrder.product;
        updatedMarketplaceOrder.type = validHedgeOrder.type;
        updatedMarketplaceOrder.supplyProfile = validHedgeOrder.supplyProfile;
        updatedMarketplaceOrder.acceptance = validHedgeOrder.acceptance;
        updatedMarketplaceOrder.crop = validHedgeOrder.crop;
        updatedMarketplaceOrder.variety = validHedgeOrder.variety;
        updatedMarketplaceOrder.cropQuantity = validHedgeOrder.cropQuantity;
        updatedMarketplaceOrder.contractedQuantity = validHedgeOrder.contractedQuantity;
        updatedMarketplaceOrder.cropQuantityUnit = validHedgeOrder.cropQuantityUnit;
        updatedMarketplaceOrder.cropQuality = validHedgeOrder.cropQuality;
        updatedMarketplaceOrder.supportedShippingProviders = validHedgeOrder.supportedShippingProviders;
        updatedMarketplaceOrder.deliveryWindowStartAt = validHedgeOrder.deliveryWindowStartAt;
        updatedMarketplaceOrder.deliveryWindowEndAt = validHedgeOrder.deliveryWindowEndAt;
        updatedMarketplaceOrder.expiresAt = validHedgeOrder.expiresAt;
        updatedMarketplaceOrder.addressId = validHedgeOrder.addressId;
        updatedMarketplaceOrder.address = validHedgeOrder.address;
        updatedMarketplaceOrder.maxRadiusMiles = validHedgeOrder.maxRadiusMiles;
        updatedMarketplaceOrder.cashPrice = validHedgeOrder.cashPrice;
        updatedMarketplaceOrder.cashPriceCurrencyCode = validHedgeOrder.cashPriceCurrencyCode;
        updatedMarketplaceOrder.basisValue = validHedgeOrder.basisValue;
        updatedMarketplaceOrder.basisYear = validHedgeOrder.basisYear;
        updatedMarketplaceOrder.basisMonthCode = validHedgeOrder.basisMonthCode;
        updatedMarketplaceOrder.createdBy = validHedgeOrder.createdBy;
        updatedMarketplaceOrder.closedBy = validHedgeOrder.closedBy;
        updatedMarketplaceOrder.closedAt = validHedgeOrder.closedAt;
        updatedMarketplaceOrder.createdAt = validHedgeOrder.createdAt;
        updatedMarketplaceOrder.updatedAt = validHedgeOrder.updatedAt;
        updatedMarketplaceOrder.notes = validHedgeOrder.notes;
        updatedMarketplaceOrder.accountName = validHedgeOrder.accountName;
        updatedMarketplaceOrder.originatingDemandOrderId = validHedgeOrder.originatingDemandOrderId;
        updatedMarketplaceOrder.originatingDemandOrder = validHedgeOrder.originatingDemandOrder;

        updatedMarketplaceOrder.status = attrs.status;
        updatedMarketplaceOrder.filledQuantity = attrs.filledQuantity;
        updatedMarketplaceOrder.futuresReferencePrice = attrs.futuresReferencePrice;

        return updatedMarketplaceOrder;
      });

      mockPricingServiceGetPricingCapabilities = jest.fn(crops => {
        return pricingCapabilityResultsMap;
      });

      MarketplaceOrderDAL.updateHedgeOrder = mockMarketplaceOrderDalUpdateHedgeOrder;
      OrderFeeDAL.createOrderFee = mockCreateOrderFee;
      MarketplaceOrderDAL.findByUid = mockMarketplaceOrderDalFindByUid;
      PricingService.getPricingCapabilities = mockPricingServiceGetPricingCapabilities;

      MarketplaceOrderNotificationService.sendFillHedgeOrderNotification = mockSendFillHedgeOrderNotification;
    });

    afterEach(() => {
      mockMarketplaceOrderDalUpdateHedgeOrder.mockReset();
      mockCreateOrderFee.mockReset();
      mockMarketplaceOrderDalFindByUid.mockReset();
      mockPricingServiceGetPricingCapabilities.mockReset();
      mockSendFillHedgeOrderNotification.mockReset();
    });

    afterAll(() => {
      MarketplaceOrderDAL.updateHedgeOrder = originalMarketplaceOrderDalUpdateHedgeOrder;
      OrderFeeDAL.createOrderFee = originalCreateOrderFee;
      MarketplaceOrderDAL.findByUid = originalMarketplaceOrderDalFindByUid;
      PricingService.getPricingCapabilities = originalPricingServiceGetPricingCapabilities;

      MarketplaceOrderNotificationService.sendFillHedgeOrderNotification = originalSendFillHedgeOrderNotification;
    });

    describe('validation', () => {
      it('should throw an error if the marketplaceOrder is not found', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'not_found',
          filledQuantity: 2000,
          hedgeFee: defaultFee
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(
            `Cannot find MarketplaceOrder with id: ${input.marketplaceOrderId}. Fill hedge failed.`
          );
        }
      });

      it('should throw an error if the marketplaceOrder is not a HEDGE product', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'otc',
          filledQuantity: 2000,
          hedgeFee: defaultFee
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(
            `Marketplace order ${input.marketplaceOrderId} cannot be filled because it is not a hedge order.`
          );
        }
      });

      it('should throw an error if the marketplaceOrder is already filled', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'filled',
          filledQuantity: 2000,
          hedgeFee: defaultFee
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(`Marketplace order ${input.marketplaceOrderId} is already filled.`);
        }
      });

      it('should throw an error if the marketplaceOrder is not WORKING', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'closed',
          filledQuantity: 2000,
          hedgeFee: defaultFee
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(
            `Marketplace order ${input.marketplaceOrderId} is not in WORKING status but is ${closedHedgeOrder.status}.`
          );
        }
      });

      it('should throw an error if the filledQuantity is larger than the marketplaceOrder cropQuantity', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'valid',
          filledQuantity: 2000,
          hedgeFee: defaultFee
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(
            `Marketplace order ${input.marketplaceOrderId} is cannot be filled for ${
              input.filledQuantity
            } over the cropQuantity ${cropQuantity}.`
          );
        }
      });

      it('should throw an error if the fee value is less than 0', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'valid',
          filledQuantity: 500,
          hedgeFee: {
            code: 'USD',
            value: -0.3
          }
        };

        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          expect(e.message).toEqual(
            `Hedge fee for filling order ${input.marketplaceOrderId} is invalid: ${input.hedgeFee}`
          );
        }
      });

      it('should not throw an error if the filledQuantity matches the marketplaceOrder cropQuantity', async () => {
        expect.assertions(1);

        const input = {
          marketplaceOrderId: 'valid',
          filledQuantity: cropQuantity,
          hedgeFee: defaultFee
        };

        let caughtError: Error = undefined;
        try {
          await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
        } catch (e) {
          log.info('error:' + JSON.stringify(e));
          caughtError = e;
        }
        expect(caughtError).toBe(undefined);
      });
    });

    it('should update filledQuantity and status', async () => {
      expect.assertions(3);

      const input = {
        marketplaceOrderId: 'valid',
        filledQuantity: 500,
        hedgeFee: defaultFee
      };

      const result: MarketplaceOrderDTO = await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);

      expect(result.filledQuantity).toEqual(input.filledQuantity);
      expect(result.status).toEqual(MarketOrderStatus.FILLED);

      expect(mockSendFillHedgeOrderNotification).toBeCalledTimes(1);
    });

    it('Should update futuresReferencePrice if one is provided', async () => {
      expect.assertions(2);
      const newFuturesReferencePrice = 5.22;
      const input: FillHedgeOrderInput = {
        marketplaceOrderId: 'valid',
        filledQuantity: 400,
        hedgeFee: defaultFee,
        filledFuturesReferencePriceValue: newFuturesReferencePrice
      };
      const result: MarketplaceOrderDTO = await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
      const resultPrice = result.price as BasisPrice;
      expect(resultPrice.lockedInPrice).toEqual(newFuturesReferencePrice);
      expect(mockMarketplaceOrderDalUpdateHedgeOrder).toHaveBeenCalledWith(
        validHedgeOrder.uid,
        {
          filledQuantity: input.filledQuantity,
          status: MarketOrderStatus.FILLED,
          futuresReferencePrice: newFuturesReferencePrice
        },
        expect.any(Transaction)
      );
    });

    it('Should leave futuresReferencePrice unchanged if one is not provided', async () => {
      expect.assertions(1);
      const input: FillHedgeOrderInput = {
        marketplaceOrderId: 'valid',
        filledQuantity: 400,
        hedgeFee: defaultFee
      };
      await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);
      expect(mockMarketplaceOrderDalUpdateHedgeOrder).toHaveBeenCalledWith(
        validHedgeOrder.uid,
        {
          filledQuantity: input.filledQuantity,
          status: MarketOrderStatus.FILLED
        },
        expect.any(Transaction)
      );
    });

    it('should create an OrderFee record', async () => {
      expect.assertions(3);

      const input = {
        marketplaceOrderId: 'valid',
        filledQuantity: 500,
        hedgeFee: defaultFee
      };

      await MarketplaceOrderService.fillHedgeOrder(input, dummyContext);

      expect(mockCreateOrderFee).toBeCalledTimes(1);
      expect(mockCreateOrderFee.mock.calls[0][0]).toEqual({
        fee: defaultFee,
        feeType: OrderFeeType.HEDGE,
        marketplaceOrderId: validHedgeOrder.id,
        payor: OrderFeePayor.SELLER
      });

      expect(mockSendFillHedgeOrderNotification).toBeCalledTimes(1);
    });
  });

  // TODO: add tests for: convertCreateHedgeOrderInputToModel
  // TODO: add tests for: convertCreateGrowerMarketplaceOrderInputToModel
  // TODO: add tests for: setDefaultValuesOnMarketplaceOrderModel
  // TODO: add tests for: getAccountName

  // TODO: add tests for: growerMarketplaceOrderCreationHelper

  describe('validateExpiresAt', () => {
    it('should fail when expiresAt is in the past', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithCashPrice', {
        expiresAt: moment.utc().subtract(1, 'day')
      });

      const result: any = MarketplaceOrderService.validateExpiresAt(model);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.EXPIRES_AT_IN_THE_PAST);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with an expiresAt in the past ${JSON.stringify(model.expiresAt)}`
      );
    });
  });

  describe('validateDeliveryDate', () => {
    it('should fail when delivery end is after delivery start', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithCashPrice', {
        deliveryWindowStartAt: moment.utc().add(7, 'days'),
        deliveryWindowEndAt: moment.utc().add(1, 'days')
      });

      const result: any = MarketplaceOrderService.validateDeliveryDate(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.DELIVERY_END_BEFORE_DELIVERY_START);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with a delivery end date: ${JSON.stringify(
          model.deliveryWindowEndAt
        )} before` + ` the delivery start date: ${JSON.stringify(model.deliveryWindowStartAt)}`
      );
    });
  });

  describe('validateOTCPricing', () => {
    it('should throw an error when no basisInput or cashInput is defined', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithCashPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.basisInput = null;
      input.price.cashInput = null;

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: false,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.CASH_OR_BASIS_REQUIRED);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with neither cash nor basis data: ${JSON.stringify(input)}`
      );
    });

    it('should throw an error when both basisInput and cashInput are defined', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithCashPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.basisInput = {
        month: FuturesMonthCode.F,
        year: 2019,
        value: 0.25
      };

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: false,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.CASH_AND_BASIS_FORBIDDEN);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with cash and basis data: ${JSON.stringify(input)}`
      );
    });

    it('should throw an error if basisInput is defined but basis pricing is not supported for the crop', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithBasisPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.cashInput = null;

      const supportedPricingModel: SupportedPricingModel = new SupportedPricingModel();
      supportedPricingModel.crop = model.crop;
      supportedPricingModel.defaultPricingType = CropPricingType.CASH;
      supportedPricingModel.supportedPricingTypes = [CropPricingType.CASH];
      const supportedPricing = {
        [model.crop]: supportedPricingModel
      };

      const basisDetail: BasisDetailsModel = new BasisDetailsModel();
      basisDetail.crop = model.crop;
      basisDetail.futuresMonthCodes = [FuturesMonthCode.F];
      basisDetail.exchange = 'test';
      basisDetail.ticker = 'TST';
      const basisDetails = {
        [model.crop]: basisDetail
      };

      const cropCalendar: CropCalendarModel = new CropCalendarModel();
      const calendarItems = {
        [model.crop]: [cropCalendar]
      };

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: true,
        newOrderPricing: new PricingCapabilitiesDTO(model.crop, supportedPricing, basisDetails, calendarItems)
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.BASIS_NOT_SUPPORTED_FOR_CROP);
      expect(result.message).toEqual(
        `Cannot create marketplace order.  Basis pricing is not supported for crop ${input.crop}. ${JSON.stringify(
          input
        )}`
      );
    });

    it('should throw an error if basisInput is defined but basis pricing is not supported for the crop - no newOrderPricing', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithBasisPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.cashInput = null;

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: true,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.BASIS_NOT_SUPPORTED_FOR_CROP);
      expect(result.message).toEqual(
        `Cannot create marketplace order.  Basis pricing is not supported for crop ${input.crop}. ${JSON.stringify(
          input
        )}`
      );
    });

    it('should throw an error if the cashPrice is negative', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithCashPrice', {
        cashPrice: -1.0,
        currencyCode: 'USD'
      });
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.basisInput = null;

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: false,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.NON_POSITIVE_CASH_VALUE);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with negative or 0 cash value: ${JSON.stringify(input)}`
      );
    });

    it('should throw an error if the cashPrice is zero', async () => {
      expect.assertions(3);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithCashPrice', {
        cashPrice: 0,
        currencyCode: 'USD'
      });
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.basisInput = null;

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: false,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.NON_POSITIVE_CASH_VALUE);
      expect(result.message).toEqual(
        `Cannot create a marketplace order with negative or 0 cash value: ${JSON.stringify(input)}`
      );
    });

    it('should not throw an error when a valid cashInput is defined', async () => {
      expect.assertions(1);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithCashPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.basisInput = null;

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: false,
        newOrderPricing: null
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeUndefined();
    });

    it('should not throw an error when a valid basisInput is defined', async () => {
      expect.assertions(1);

      const model: MarketplaceOrderModel = await factory.build('MarketplaceOrderWithBasisPrice');
      const input: CreateGrowerMarketplaceOrderInput = model.toGrowerOfferCreateInput();
      input.price.cashInput = null;

      const supportedPricingModel: SupportedPricingModel = new SupportedPricingModel();
      supportedPricingModel.crop = model.crop;
      supportedPricingModel.defaultPricingType = CropPricingType.CASH;
      supportedPricingModel.supportedPricingTypes = [CropPricingType.BASIS, CropPricingType.CASH];
      const supportedPricing = {
        [model.crop]: supportedPricingModel
      };

      const basisDetail: BasisDetailsModel = new BasisDetailsModel();
      basisDetail.crop = model.crop;
      basisDetail.futuresMonthCodes = [FuturesMonthCode.F];
      basisDetail.exchange = 'test';
      basisDetail.ticker = 'TST';
      const basisDetails = {
        [model.crop]: basisDetail
      };

      const cropCalendar: CropCalendarModel = new CropCalendarModel();
      const calendarItems = {
        [model.crop]: [cropCalendar]
      };

      const pricingDetails: CreateInputPricingDetails = {
        isBasisOffer: true,
        newOrderPricing: new PricingCapabilitiesDTO(model.crop, supportedPricing, basisDetails, calendarItems)
      };

      const result: any = MarketplaceOrderService.validateOTCPricing(input, pricingDetails);

      expect(result).toBeUndefined();
    });
  });

  describe('validateFutureReferencePrice', () => {
    it('should fail when futuresReferencePrice === 0', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 0,
        basisYear: 2019,
        basisMonthCode: FuturesMonthCode.M
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should fail when futuresReferencePrice < 0', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: -0.1,
        basisYear: 2019,
        basisMonthCode: FuturesMonthCode.M
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should fail when basisMonthCode is null', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 1.0,
        basisYear: 2019,
        basisMonthCode: null
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should fail when basisMonthCode is undefined', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 1.0,
        basisYear: 2019,
        basisMonthCode: undefined
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should fail when basisYear is null', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 1.0,
        basisYear: null,
        basisMonthCode: FuturesMonthCode.M
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should fail when basisYear is undefined', async () => {
      expect.assertions(3);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 1.0,
        basisYear: undefined,
        basisMonthCode: FuturesMonthCode.M
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeDefined();
      expect(result.code).toEqual(MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE);
      expect(result.message).toEqual(
        `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      );
    });

    it('should not throw an error for a valid future referencePrice', async () => {
      expect.assertions(1);

      const model = await factory.build('MarketplaceOrderWithFloatingBasis', {
        futuresReferencePrice: 1.0,
        basisYear: 2020,
        basisMonthCode: FuturesMonthCode.M
      });

      const result: any = MarketplaceOrderService.validateFutureReferencePrice(model);
      expect(result).toBeUndefined();
    });
  });
});
