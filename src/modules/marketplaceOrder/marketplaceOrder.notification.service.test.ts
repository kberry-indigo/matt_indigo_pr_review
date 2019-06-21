import {
  cropLabel,
  dateTimeString,
  EmailService,
  getCommaSeparatedDollars,
  MarketplaceOrderEmailData,
  numberWithCommas,
  SellerActingCapacity,
  toTitleCase,
  UserService
} from '../../services';
import {
  Account,
  Crop,
  FuturesMonthCode,
  FuturesMonthCodeShortMonths,
  MarketOrderProduct,
  MarketplaceOrderExpirationType
} from '@indigo-ag/schema';
import AddressModel from '../addresses/address.model';
import MarketplaceOrderModel from './marketplaceOrder.model';
import { factory } from 'factory-girl';
import { GMAService, SaleType } from '../supply/gma.service';
import { CropCalendarDAL } from '../pricing/cropCalendar.dal';
import CropCalendarModel from '../pricing/cropCalendar.model';
import { AddressDAL } from '../addresses';
import { MarketplaceSupplyProfileDAL } from '../supply';
import MarketplaceSupplyProfileModel from '../supply/profile.model';
import { Context, User } from '@indigo-ag/server';
import {
  MarketplaceOrderMessageType,
  MarketplaceOrderNotificationService
} from './marketplaceOrder.notification.service';
import { generateDummyContext, NON_GMA_USER } from '../../../test/fixtures/context.fixture';
import { databaseTestFramework } from '../../../test/framework/database';

const dummyUser: User = NON_GMA_USER;
const dummyContext: Context = generateDummyContext(dummyUser);

describe('MarketplaceOrderNotificationService', () => {
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

  afterEach(async () => {
    factory.created.clear();
  });

  describe('generateGenericEmailData', () => {
    const originalGetFirstHoldingDate = MarketplaceOrderNotificationService.getFirstHoldingDate;
    let mockGetFirstHoldingDate = jest.fn();

    const originalGetSellerActingCapacity = UserService.getSellerActingCapacity;
    let mockGetSellerActingCapacity = jest.fn();

    const knownCalendarCrop: Crop = Crop.BARLEY;
    const createdAt = new Date(2019, 5, 17, 15, 12);
    const firstHoldingDate: Date = new Date(2019, 10, 5);

    let address: AddressModel = null;
    let cashOrder: MarketplaceOrderModel = null;
    let hedgeOrder: MarketplaceOrderModel = null;

    let account: Account = {
      id: dummyUser.primaryAccountId,
      name: 'name',
      owner: {
        email: dummyUser.email,
        id: dummyUser.id
      }
    } as Account;
    const messageType: MarketplaceOrderMessageType = MarketplaceOrderMessageType.Created;
    const isUserGma: boolean = true;
    const connectedGmaEmail: string = 'connectedGmaEmail';
    const sellerActingCapacity: SellerActingCapacity = SellerActingCapacity.PRINCIPAL;

    beforeEach(async () => {
      address = await factory.build('Address');
      cashOrder = await factory.build('MarketplaceOrder', {
        createdAt,
        expirationType: MarketplaceOrderExpirationType.GOOD_TIL_CANCEL,
        uid: 'abc123'
      });
      cashOrder.address = address;
      hedgeOrder = await factory.build('MarketplaceOrderWithFloatingBasis', {
        createdAt,
        crop: knownCalendarCrop,
        expirationType: MarketplaceOrderExpirationType.EXPIRATION_DATE
      });
      hedgeOrder.address = address;

      mockGetFirstHoldingDate = jest.fn((crop: Crop, month: FuturesMonthCode, year: number) => {
        if (crop === knownCalendarCrop) {
          return firstHoldingDate;
        } else {
          return null;
        }
      });
      mockGetSellerActingCapacity = jest.fn((sellerUserId: string, supplyOwnerUserId: string) => {
        return sellerActingCapacity;
      });

      MarketplaceOrderNotificationService.getFirstHoldingDate = mockGetFirstHoldingDate;
      UserService.getSellerActingCapacity = mockGetSellerActingCapacity;
    });

    afterEach(() => {
      mockGetFirstHoldingDate.mockReset();
      mockGetSellerActingCapacity.mockReset();
    });

    afterAll(() => {
      MarketplaceOrderNotificationService.getFirstHoldingDate = originalGetFirstHoldingDate;
      UserService.getSellerActingCapacity = originalGetSellerActingCapacity;
    });

    it('should construct a MarketplaceOrderEmailData object', async () => {
      expect.assertions(23);

      const result: MarketplaceOrderEmailData = await MarketplaceOrderNotificationService.generateGenericEmailData(
        cashOrder,
        account,
        messageType,
        dummyContext,
        isUserGma,
        connectedGmaEmail
      );

      expect(mockGetFirstHoldingDate).not.toHaveBeenCalled();
      expect(mockGetSellerActingCapacity).toHaveBeenCalled();

      expect(result.connectedGmaEmail).toEqual(connectedGmaEmail);
      expect(result.createdAt).toEqual(dateTimeString(cashOrder.createdAt));
      expect(result.cropLabel).toEqual(cropLabel(cashOrder.crop));
      expect(result.cropQuantity).toEqual(numberWithCommas(cashOrder.cropQuantity));
      expect(result.cropQuantityUnit).toEqual(toTitleCase(cashOrder.cropQuantityUnit));
      expect(result.deliveryMethod).toEqual('Buyer');
      expect(result.expiresAt).toEqual(`Good til cancel (${dateTimeString(hedgeOrder.expiresAt)})`);
      expect(result.firstHoldingDate).toEqual('');
      expect(result.growerAccountName).toEqual(account.name);
      expect(result.growerAddress).toEqual(`${cashOrder.address.city}, ${cashOrder.address.state}`);
      expect(result.growerEmail).toEqual(account.owner.email);
      expect(result.isSellerGma).toEqual(isUserGma);
      expect(result.messageType).toEqual(messageType);
      expect(result.price).toEqual(getCommaSeparatedDollars(cashOrder.cashPrice));
      expect(result.pricingType).toEqual('Cash');
      expect(result.referenceId).toEqual(cashOrder.referenceId);
      expect(result.saleType).toEqual(SaleType.SELLER_INITIATED);
      expect(result.sellerActingCapacity).toEqual(sellerActingCapacity);
      expect(result.sellerEmail).toEqual(dummyContext.user.email);
      expect(result.sellerName).toEqual(`${dummyContext.user.firstName} ${dummyContext.user.lastName}`);
      expect(result.uid).toEqual(cashOrder.uid);
    });

    it('should retrieve firstHoldingDate if a basis order', async () => {
      expect.assertions(4);

      const result: MarketplaceOrderEmailData = await MarketplaceOrderNotificationService.generateGenericEmailData(
        hedgeOrder,
        account,
        messageType,
        dummyContext,
        isUserGma,
        connectedGmaEmail
      );

      expect(mockGetFirstHoldingDate).toHaveBeenCalled();
      expect(mockGetSellerActingCapacity).toHaveBeenCalled();

      expect(result.firstHoldingDate).toEqual(dateTimeString(firstHoldingDate));
      expect(result.pricingType).toEqual('Basis');
    });

    it('should return an empty string for firstHoldingDate if not found', async () => {
      expect.assertions(3);

      hedgeOrder.crop = Crop.CORN;

      const result: MarketplaceOrderEmailData = await MarketplaceOrderNotificationService.generateGenericEmailData(
        hedgeOrder,
        account,
        messageType,
        dummyContext,
        isUserGma,
        connectedGmaEmail
      );

      expect(mockGetFirstHoldingDate).toHaveBeenCalled();
      expect(mockGetSellerActingCapacity).toHaveBeenCalled();

      expect(result.firstHoldingDate).toEqual('');
    });

    it('should return the expiration date if order.expirationType === MarketplaceOrderExpirationType.EXPIRATION_DATE', async () => {
      hedgeOrder.expirationType = MarketplaceOrderExpirationType.EXPIRATION_DATE;

      const result: MarketplaceOrderEmailData = await MarketplaceOrderNotificationService.generateGenericEmailData(
        hedgeOrder,
        account,
        messageType,
        dummyContext,
        isUserGma,
        connectedGmaEmail
      );

      expect(result.expiresAt).toEqual(dateTimeString(hedgeOrder.expiresAt));
    });

    it('should return Good til cancel (${dateTimeString(hedgeOrder.expiresAt)}) if order.expirationType === MarketplaceOrderExpirationType.GOOD_TIL_CANCEL', async () => {
      hedgeOrder.expirationType = MarketplaceOrderExpirationType.GOOD_TIL_CANCEL;

      const result: MarketplaceOrderEmailData = await MarketplaceOrderNotificationService.generateGenericEmailData(
        hedgeOrder,
        account,
        messageType,
        dummyContext,
        isUserGma,
        connectedGmaEmail
      );

      expect(result.expiresAt).toEqual(`Good til cancel (${dateTimeString(hedgeOrder.expiresAt)})`);
    });
  });

  describe('determineEmailFunction', () => {
    it('should return sendMarketplaceOrderClosed if messageType is Closed', () => {
      expect.assertions(2);

      const otcResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Closed,
        MarketOrderProduct.OTC
      );
      expect(otcResult).toEqual(EmailService.prototype.sendMarketplaceOrderClosed);

      const hedgeResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Closed,
        MarketOrderProduct.HEDGE
      );
      expect(hedgeResult).toEqual(EmailService.prototype.sendMarketplaceOrderClosed);
    });

    it('should return sendHedgeOrderCreated if messageType !== Closed and product === HEDGE', () => {
      expect.assertions(2);

      const createdResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Created,
        MarketOrderProduct.HEDGE
      );
      expect(createdResult).toEqual(EmailService.prototype.sendHedgeOrderCreated);

      const updatedResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Updated,
        MarketOrderProduct.HEDGE
      );
      expect(updatedResult).toEqual(EmailService.prototype.sendHedgeOrderCreated);
    });

    it('should return sendMarketplaceOrderCreated if messageType !== Closed and product !== HEDGE', () => {
      expect.assertions(2);

      const createdResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Created,
        MarketOrderProduct.OTC
      );
      expect(createdResult).toEqual(EmailService.prototype.sendMarketplaceOrderCreated);

      const updatedResult = MarketplaceOrderNotificationService.determineEmailFunction(
        MarketplaceOrderMessageType.Updated,
        MarketOrderProduct.OTC
      );
      expect(updatedResult).toEqual(EmailService.prototype.sendMarketplaceOrderCreated);
    });
  });

  describe('sendSupportEmail', () => {
    const mockSendMarketplaceOrderClosedToSupport = jest.fn();
    const mockSendMarketplaceOrderCreatedToSupport = jest.fn();
    const mockSendHedgeOrderCreatedToSupport = jest.fn();

    let emailService: EmailService = new EmailService();
    let emailData = {} as MarketplaceOrderEmailData;

    beforeEach(() => {
      emailService.sendMarketplaceOrderClosedToSupport = mockSendMarketplaceOrderClosedToSupport;
      emailService.sendMarketplaceOrderCreatedToSupport = mockSendMarketplaceOrderCreatedToSupport;
      emailService.sendHedgeOrderCreatedToSupport = mockSendHedgeOrderCreatedToSupport;
    });

    afterEach(() => {
      mockSendMarketplaceOrderClosedToSupport.mockReset();
      mockSendMarketplaceOrderCreatedToSupport.mockReset();
      mockSendHedgeOrderCreatedToSupport.mockReset();
    });

    it('should call emailService.sendMarketplaceOrderClosedToSupport when messageType: CLOSED and product: OTC', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Closed,
        MarketOrderProduct.OTC,
        mockSendMarketplaceOrderClosedToSupport
      );
    });

    it('should call emailService.sendMarketplaceOrderClosedToSupport when messageType: Closed and product: HEDGE', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Closed,
        MarketOrderProduct.HEDGE,
        mockSendMarketplaceOrderClosedToSupport
      );
    });

    it('should call emailService.sendMarketplaceOrderCreatedToSupport when messageType: Created and product: OTC', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Created,
        MarketOrderProduct.OTC,
        mockSendMarketplaceOrderCreatedToSupport
      );
    });

    it('should call emailService.sendHedgeOrderCreatedToSupport when messageType: Created and product: HEDGE', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Created,
        MarketOrderProduct.HEDGE,
        mockSendHedgeOrderCreatedToSupport
      );
    });

    it('should call emailService.sendMarketplaceOrderCreatedToSupport when messageType: Updated and product: OTC', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Updated,
        MarketOrderProduct.OTC,
        mockSendMarketplaceOrderCreatedToSupport
      );
    });

    it('should call emailService.sendMarketplaceOrderCreatedToSupport when messageType: Updated and product: HEDGE', () => {
      validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
        emailService,
        emailData,
        MarketplaceOrderMessageType.Updated,
        MarketOrderProduct.HEDGE,
        mockSendMarketplaceOrderCreatedToSupport
      );
    });
  });

  describe('getFirstHoldingDate', () => {
    const originalFindCropCalendarModel = CropCalendarDAL.findCropCalendarModel;
    let mockFindCropCalendarModel = jest.fn();

    let cropCalendarModel: CropCalendarModel = null;

    const crop: Crop = Crop.CORN;
    const contractMonth: FuturesMonthCode = FuturesMonthCode.F;
    const contractYear: number = 2019;
    const firstHolding: Date = new Date(2019, 10, 5);

    beforeEach(async () => {
      cropCalendarModel = factory.build('CropCalendar', {
        crop,
        contractMonth: FuturesMonthCodeShortMonths[contractMonth],
        contractYear,
        firstHolding
      });

      mockFindCropCalendarModel = jest.fn((crop: Crop, month: FuturesMonthCode, year: number) => {
        if (crop === crop && month === contractMonth && contractYear === year) {
          return cropCalendarModel;
        } else {
          return null;
        }
      });

      CropCalendarDAL.findCropCalendarModel = mockFindCropCalendarModel;
    });

    afterEach(() => {
      mockFindCropCalendarModel.mockReset();
    });

    afterAll(() => {
      CropCalendarDAL.findCropCalendarModel = originalFindCropCalendarModel;
    });

    it('should return the firstHolding date from the CropCalendarModel if one exists', async () => {
      expect.assertions(1);

      const firstHoldingDate: Date = await MarketplaceOrderNotificationService.getFirstHoldingDate(
        crop,
        contractMonth,
        contractYear
      );

      expect(firstHoldingDate).toEqual(firstHolding);
    });

    it('should return the null if no matching CropCalendarModel exists', async () => {
      expect.assertions(1);

      const firstHoldingDate: Date | null = await MarketplaceOrderNotificationService.getFirstHoldingDate(
        crop,
        contractMonth,
        1900
      );

      expect(firstHoldingDate).toEqual(null);
    });
  });

  describe('sendFillHedgeOrderNotification', () => {
    const originalSendConfirmationEmail = MarketplaceOrderNotificationService.sendConfirmationEmail;
    let mockSendConfirmationEmail = jest.fn();

    const originalAddressDAlFindById = AddressDAL.findById;
    let mockAddressDAlFindById = jest.fn();

    const originalMarketplaceSupplyProfileDALfindById = MarketplaceSupplyProfileDAL.findById;
    let mockMarketplaceSupplyProfileDALfindById = jest.fn();

    const originalGetAccount = UserService.getAccount;
    let mockGetAccount = jest.fn();

    const originalIsContextUserGma = GMAService.isContextUserGma;
    let mockIsContextUserGma = jest.fn();

    let hedgeOrder: MarketplaceOrderModel = null;
    let address: AddressModel = null;
    let supplyProfile: MarketplaceSupplyProfileModel = null;
    let account: Account = {
      id: dummyUser.primaryAccountId,
      name: 'name',
      owner: {
        email: dummyUser.email,
        id: dummyUser.id
      }
    } as Account;
    const isUserGma = true;

    beforeEach(async () => {
      address = await factory.build('Address');
      hedgeOrder = await factory.build('MarketplaceOrderWithFloatingBasis');
      hedgeOrder.address = address;
      supplyProfile = await factory.build('MarketplaceSupplyProfileWithProductionLocation');

      mockAddressDAlFindById = jest.fn((id: number) => {
        return address;
      });

      mockMarketplaceSupplyProfileDALfindById = jest.fn((id: number) => {
        return supplyProfile;
      });

      mockGetAccount = jest.fn((context: Context, accountId: string, selectionSet?: string) => {
        return account;
      });

      mockIsContextUserGma = jest.fn((context: Context) => {
        return isUserGma;
      });

      MarketplaceOrderNotificationService.sendConfirmationEmail = mockSendConfirmationEmail;
      AddressDAL.findById = mockAddressDAlFindById;
      MarketplaceSupplyProfileDAL.findById = mockMarketplaceSupplyProfileDALfindById;
      UserService.getAccount = mockGetAccount;
      GMAService.isContextUserGma = mockIsContextUserGma;
    });

    afterEach(() => {
      MarketplaceOrderNotificationService.sendConfirmationEmail = originalSendConfirmationEmail;
      AddressDAL.findById = originalAddressDAlFindById;
      MarketplaceSupplyProfileDAL.findById = originalMarketplaceSupplyProfileDALfindById;
      UserService.getAccount = originalGetAccount;
      GMAService.isContextUserGma = originalIsContextUserGma;
    });

    it('should call sendConfirmationEmail with the correct information', async () => {
      expect.assertions(12);

      await MarketplaceOrderNotificationService.sendFillHedgeOrderNotification(dummyContext, hedgeOrder);

      expect(mockAddressDAlFindById).toBeCalled();
      expect(mockAddressDAlFindById).toReturnWith(address);

      expect(mockMarketplaceSupplyProfileDALfindById).toBeCalled();
      expect(mockMarketplaceSupplyProfileDALfindById).toReturnWith(supplyProfile);

      expect(mockGetAccount).toBeCalled();
      expect(mockGetAccount).toReturnWith(account);

      expect(mockSendConfirmationEmail).toBeCalled();
      expect(mockSendConfirmationEmail.mock.calls[0][0]).toEqual(MarketplaceOrderMessageType.Filled);
      expect(mockSendConfirmationEmail.mock.calls[0][1]).toEqual(hedgeOrder);
      expect(mockSendConfirmationEmail.mock.calls[0][2]).toEqual(account);
      expect(mockSendConfirmationEmail.mock.calls[0][3]).toEqual(isUserGma);
      expect(mockSendConfirmationEmail.mock.calls[0][4]).toEqual(dummyContext);
    });
  });
});

function validateCorrectEmailServiceMethodCalledWhenSendingSupportEmail(
  emailService: EmailService,
  emailData: MarketplaceOrderEmailData,
  messageType: MarketplaceOrderMessageType,
  product: MarketOrderProduct,
  expectedMock: any
) {
  expect.assertions(1);

  MarketplaceOrderNotificationService.sendSupportEmail(emailService, emailData, messageType, product);
  expect(expectedMock).toBeCalled();
}
