import {
  Account,
  Crop,
  FuturesMonthCode,
  MarketOrderProduct,
  MarketplaceOrderExpirationType,
  ShippingProvider
} from '@indigo-ag/schema';
import { Context, ErrorReporter } from '@indigo-ag/server';
import {
  cropLabel,
  dateTimeString,
  EmailService,
  getCommaSeparatedDollars,
  getPriceValue,
  joinWithOr,
  MarketplaceOrderEmailData,
  numberWithCommas,
  toTitleCase,
  UserService
} from '../../services';
import { AddressDAL } from '../addresses';
import { CropCalendarDAL } from '../pricing/cropCalendar.dal';
import CropCalendarModel from '../pricing/cropCalendar.model';
import { MarketplaceSupplyProfileDAL, MarketplaceSupplyProfileModel } from '../supply';
import { GMAService, SaleType } from '../supply/gma.service';
import MarketplaceOrderModel from './marketplaceOrder.model';

export enum MarketplaceOrderMessageType {
  Closed,
  Created,
  Filled,
  Updated
}

export class MarketplaceOrderNotificationService {
  static async sendConfirmationEmail(
    messageType: MarketplaceOrderMessageType,
    marketplaceOrderModel: MarketplaceOrderModel,
    account: Account,
    isUserGma: boolean,
    context: Context
  ) {
    const connectedGmaUser = await GMAService.getAssociatedGMAUser(account.id, context);
    const connectedGmaUserEmail: string = connectedGmaUser ? connectedGmaUser.email : null;

    const genericEmailData: MarketplaceOrderEmailData = await this.generateGenericEmailData(
      marketplaceOrderModel,
      account,
      messageType,
      context,
      isUserGma,
      connectedGmaUserEmail
    );

    const emailService = new EmailService();

    // Send emails to grower entities.
    log.info(
      `Sending grower offer ${
        MarketplaceOrderMessageType[messageType]
      } confirmation to grower entities for accountUid ${account.id} for MarketplaceOrder: ${JSON.stringify(
        marketplaceOrderModel
      )}`
    );

    const emailFunction = this.determineEmailFunction(messageType, marketplaceOrderModel.product);

    const sentTo: string[] = await emailService.sendEmailsToAccountEdge<MarketplaceOrderEmailData>(
      genericEmailData,
      account.id,
      context,
      emailFunction
    );

    await GMAService.handleGmaGrowerEmail(genericEmailData, account.id, emailService, sentTo, emailFunction);

    log.info(
      `Sending grower offer ${MarketplaceOrderMessageType[messageType]} confirmation to support team for offer uid ${
        marketplaceOrderModel.uid
      }`
    );
    return this.sendSupportEmail(emailService, genericEmailData, messageType, marketplaceOrderModel.product);
  }

  static async generateGenericEmailData(
    order: MarketplaceOrderModel,
    account: Account,
    messageType: MarketplaceOrderMessageType,
    context: Context,
    isUserGma: boolean,
    connectedGmaEmail: string
  ): Promise<MarketplaceOrderEmailData> {
    let firstHoldingDate = null;
    if (order.isBasisOffer()) {
      firstHoldingDate = await this.getFirstHoldingDate(order.crop, order.basisMonthCode, order.basisYear);
    }

    const expiresAtString = dateTimeString(order.expiresAt);
    const genericEmailData: MarketplaceOrderEmailData = {
      connectedGmaEmail,
      createdAt: dateTimeString(order.createdAt),
      cropLabel: cropLabel(order.crop),
      cropQuantity: numberWithCommas(order.cropQuantity),
      cropQuantityUnit: toTitleCase(order.cropQuantityUnit),
      deliveryMethod: joinWithOr(
        order.supportedShippingProviders.map(provider => toTitleCase(ShippingProvider[provider]))
      ),
      expiresAt:
        order.expirationType === MarketplaceOrderExpirationType.EXPIRATION_DATE
          ? dateTimeString(order.expiresAt)
          : `Good til cancel (${expiresAtString})`,
      firstHoldingDate: firstHoldingDate ? dateTimeString(firstHoldingDate) : '',
      growerAccountName: account.name,
      growerAddress: `${order.address.city}, ${order.address.state}`,
      growerEmail: account.owner.email,
      isSellerGma: isUserGma,
      messageType,
      price: order.isBasisOffer()
        ? getPriceValue(order.basisValue, order.basisMonthCode, order.basisYear)
        : getCommaSeparatedDollars(order.cashPrice),
      pricingType: order.isBasisOffer() ? 'Basis' : 'Cash',
      referenceId: order.referenceId,
      saleType: SaleType.SELLER_INITIATED,
      sellerActingCapacity: UserService.getSellerActingCapacity(context.user.id, account.owner.id),
      sellerEmail: context.user.email,
      sellerName: `${context.user.firstName} ${context.user.lastName}`,
      uid: order.uid
    };

    return genericEmailData;
  }

  static determineEmailFunction(messageType: MarketplaceOrderMessageType, product: MarketOrderProduct) {
    let emailFunction = null;
    if (messageType === MarketplaceOrderMessageType.Closed) {
      emailFunction = EmailService.prototype.sendMarketplaceOrderClosed;
    } else if (messageType === MarketplaceOrderMessageType.Filled) {
      emailFunction = EmailService.prototype.sendHedgeOrderFilled;
    } else {
      emailFunction =
        product === MarketOrderProduct.HEDGE
          ? EmailService.prototype.sendHedgeOrderCreated
          : EmailService.prototype.sendMarketplaceOrderCreated;
    }
    return emailFunction;
  }

  /**
   * Orchestrates calling the correct emailService method depending on messageType and product.
   * @param emailService
   * @param emailData
   * @param messageType
   * @param product
   */
  static sendSupportEmail(
    emailService: EmailService,
    emailData: MarketplaceOrderEmailData,
    messageType: MarketplaceOrderMessageType,
    product: MarketOrderProduct
  ) {
    switch (messageType) {
      case MarketplaceOrderMessageType.Closed:
        return emailService.sendMarketplaceOrderClosedToSupport(emailData);
      case MarketplaceOrderMessageType.Created:
        if (product === MarketOrderProduct.HEDGE) {
          return emailService.sendHedgeOrderCreatedToSupport(emailData);
        } else {
          return emailService.sendMarketplaceOrderCreatedToSupport(emailData);
        }
      case MarketplaceOrderMessageType.Filled:
        return emailService.sendHedgeOrderFilledToSupport(emailData);
      case MarketplaceOrderMessageType.Updated:
        return emailService.sendMarketplaceOrderCreatedToSupport(emailData);
      default:
        throw Error(`Cannot send marketplace order confirmation message.  Unsupported message type ${messageType}`);
    }
  }

  static async sendFillHedgeOrderNotification(context: Context, order: MarketplaceOrderModel) {
    const orderFilled = order;

    try {
      // address is not loaded by default as part of the DAL call
      orderFilled.address = await AddressDAL.findById(orderFilled.addressId);
      const supplyProfile: MarketplaceSupplyProfileModel = await MarketplaceSupplyProfileDAL.findById(
        orderFilled.supplyProfileId
      );
      const account = await UserService.getAccount(context, supplyProfile.accountUid, `{ id name owner { id email } }`);
      const isUserGma = await GMAService.isContextUserGma(context);

      await MarketplaceOrderNotificationService.sendConfirmationEmail(
        MarketplaceOrderMessageType.Filled,
        orderFilled,
        account,
        isUserGma,
        context
      );
    } catch (err) {
      const msg = `Marketplace order was filled with id: ${
        orderFilled.uid
      } but a confirmation email failed to send with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }
  }

  static async getFirstHoldingDate(
    crop: Crop,
    contractMonth: FuturesMonthCode,
    contractYear: number
  ): Promise<Date | null> {
    const cropCalendarModel: CropCalendarModel = await CropCalendarDAL.findCropCalendarModel(
      crop,
      contractMonth,
      contractYear
    );
    return cropCalendarModel ? cropCalendarModel.firstHolding : null;
  }
}
