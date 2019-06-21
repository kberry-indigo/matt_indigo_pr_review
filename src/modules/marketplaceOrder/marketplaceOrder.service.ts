import {
  Account,
  Address,
  BidSort,
  CreateGrowerMarketplaceOrderInput,
  CreateHedgeOrderInput,
  CropPricingType,
  CropQualitySpecification,
  FillHedgeOrderInput,
  FuturesMonthCode,
  GrowerOfferForDemandImpl,
  GrowerOffersForDemandWhereInput,
  LimitOffsetPageInfo,
  LimitOffsetPaginationInput,
  MarketOrderProduct,
  MarketOrderStatus,
  MarketOrderType,
  MarketplaceOrderExpirationType,
  MarketplaceOrderOrderByInput,
  MarketplaceOrderSortBy,
  MarketplaceOrderSortInput,
  MarketplaceOrderWhereInput,
  OrderFeePayor,
  OrderFeeType,
  ShippingProvider,
  SortDirection
} from '@indigo-ag/schema';
import { Context, ErrorReporter, NestedError, TypeNotFoundError, User } from '@indigo-ag/server';
import { UserInputError } from 'apollo-server-errors';
import { isNil } from 'lodash';
import { Op } from 'sequelize';
import { IFindOptions, Sequelize } from 'sequelize-typescript';
import { MarketplaceBidAPIConnector } from '../../connectors/bid_api/connector';
import { Config } from '../../core/config';
import { DAL, ListOptions } from '../../core/dal';
import { DatabaseConnection } from '../../core/sequelize';
import { validateLimitOffsetPageInput } from '../../core/util';
import {
  cropLabel,
  dateTimeString,
  EmailService,
  getCommaSeparatedDollars,
  getPriceValue,
  joinWithOr,
  numberWithCommas,
  toTitleCase,
  UserService
} from '../../services';
import AddressModel from '../addresses/address.model';
import { OrderFeeDAL } from '../orderFees';
import { DemandOrderModel } from '../orders';
import { PricingService } from '../pricing';
import { PricingCapabilitiesDTO } from '../pricing/pricing.dto';
import { MarketplaceSupplyProfileDAL, MarketplaceSupplyProfileModel, ProductionLocationDAL } from '../supply';
import { GMAService } from '../supply/gma.service';
import { MarketplaceUserAgreementAcceptanceService } from '../userAgreementAcceptance';
import { GrowerOffersForDemandPagedResponseDTO } from './growerOffersForDemandPagedResponse.dto';
import {
  GrowerOfferUpdatableAttributes,
  HedgeOfferUpdatableAttributes,
  MarketplaceOrderDAL
} from './marketplaceOrder.dal';
import { MarketplaceOrderDTO } from './marketplaceOrder.dto';
import MarketplaceOrderModel from './marketplaceOrder.model';
import {
  MarketplaceOrderMessageType,
  MarketplaceOrderNotificationService
} from './marketplaceOrder.notification.service';

export enum MarketplaceOrderOperationErrorCode {
  ALREADY_CLOSED,
  ALREADY_FILLED,
  BASIS_NOT_SUPPORTED_FOR_CROP,
  CANNOT_CLOSE_ACCEPTED,
  CANNOT_FILL_OVER_CONTRACT,
  CASH_AND_BASIS_FORBIDDEN,
  CASH_OR_BASIS_REQUIRED,
  DELIVERY_END_BEFORE_DELIVERY_START,
  EXPIRES_AT_IN_THE_PAST,
  INVALID_STATUS_FOR_OPERATION,
  INVALID_FUTURE_REFERENCE_PRICE,
  INVALID_HEDGE_FEE,
  NON_POSITIVE_CASH_VALUE,
  NOT_FOUND,
  NOT_HEDGE_ORDER
}

interface MarketplaceOrderOperationError {
  code: MarketplaceOrderOperationErrorCode;
  message: string;
}

export interface CreateInputPricingDetails {
  isBasisOffer: boolean;
  newOrderPricing: PricingCapabilitiesDTO | null;
}

const DEFAULT_PAGINATION: LimitOffsetPaginationInput = {
  limit: 20,
  offset: 0
};

export class MarketplaceOrderService {
  public static async findByUid(uid: string) {
    const order = await MarketplaceOrderDAL.findByUid(uid);
    if (order) {
      const pricing = (await PricingService.getPricingCapabilities([order.crop]))[0];
      return new MarketplaceOrderDTO(order, pricing);
    }
  }

  /**
   * @deprecated unsafe due to no limits on results / use findPage instead
   * @param where
   * @param sort
   */
  public static async findAll(
    where: MarketplaceOrderWhereInput & { internalIds?: number[] },
    sort: MarketplaceOrderSortInput = {
      sortBy: MarketplaceOrderSortBy.DATE_CREATED,
      sortDirection: SortDirection.DESC
    }
  ) {
    const orders = await MarketplaceOrderDAL.findAll(where, sort);
    if (!orders) {
      return [];
    }
    const pricing = await PricingService.getPricingCapabilitiesMap(orders.map(order => order.crop));
    return orders.map(order => {
      return new MarketplaceOrderDTO(order, pricing[order.crop]);
    });
  }

  // PERFECTEXAMPLE_START Limit/offset paginated query using generic DAL.findAndCountAll
  public static async findPage(
    where: MarketplaceOrderWhereInput,
    orderBy: MarketplaceOrderOrderByInput[] = [],
    pagination: LimitOffsetPageInfo
  ) {
    // Pagination should be enforced at the graphQL layer going forward, once the 'marketplaceOrders' query
    // is fully deprecated this won't be necessary.
    if (!pagination) {
      pagination = DEFAULT_PAGINATION;
    }
    validateLimitOffsetPageInput(pagination);
    // We want to default product to OTC if it is not specified in the input (graphQL defaults should take care
    // of that but this way it's testable and more explicit)
    if (!where.product_in) {
      where.product_in = [MarketOrderProduct.OTC];
    }
    // TODO: refactor this once where.status is removed
    if (where.status && (where.status_in instanceof Array && where.status_in.length > 0)) {
      throw new UserInputError(
        `The endpoint does not support the use of both the status_in and the deprecated status filter at the same time.`
      );
    }

    // We need some manual handling for the status field.
    if (where.status === MarketOrderStatus.OPEN) {
      where.status_in = MarketplaceOrderModel.OPEN_ORDER_STATUSES;
      delete where.status;
    } else if (where.status === MarketOrderStatus.ACCEPTED) {
      where.status_in = MarketplaceOrderModel.ACCEPTED_ORDER_STATUSES;
      delete where.status;
    }

    let hasQuantityAvailableToContractFilter = null;
    if (where.hasQuantityAvailableToContract !== undefined) {
      hasQuantityAvailableToContractFilter = where.hasQuantityAvailableToContract;
      delete where.hasQuantityAvailableToContract;
    }
    const listOptions: ListOptions = {
      // This tells the SQL that it may need to join against another table to resolve a field for the where clause
      lookupFields: {
        state: {
          lookupField: 'state',
          model: AddressModel
        },
        supplyProfileId: {
          lookupField: 'uid',
          model: MarketplaceSupplyProfileModel
        }
      },
      orderBy,
      pagination,
      where,
      whereKeyRename: {
        ids: 'uid_in'
      }
    };

    const findOptions: IFindOptions<MarketplaceOrderModel> = DAL.createFindOptionsForList<MarketplaceOrderModel>(
      listOptions
    );

    /**
     * We custom queries to filter / sort by availableToContractQuantity a calculated property of MarketplaceOrderModel
     * which = filledQuantity - contractedQuantity
     */
    const availableToContractQuantitySql = '("filledQuantity" - "contractedQuantity")';
    if (hasQuantityAvailableToContractFilter === true) {
      findOptions.where = {
        ...findOptions.where,
        [Op.and]: [Sequelize.literal(`${availableToContractQuantitySql} > 0`)]
      };
    }
    if (hasQuantityAvailableToContractFilter === false) {
      findOptions.where = {
        ...findOptions.where,
        [Op.and]: [Sequelize.literal(`${availableToContractQuantitySql} <= 0`)]
      };
    }

    if (
      orderBy instanceof Array &&
      (orderBy.includes(MarketplaceOrderOrderByInput.availableToContractQuantity_ASC) ||
        orderBy.includes(MarketplaceOrderOrderByInput.availableToContractQuantity_DESC))
    ) {
      /**
       * We have to rebuild and replace the findOptions.order to make sure we maintain the order of
       * MarketplaceOrderOrderByInput values as we insert our Sequelize.literal entries.
       */
      findOptions.order = orderBy.map(order => {
        if (order === MarketplaceOrderOrderByInput.availableToContractQuantity_ASC) {
          return Sequelize.literal(`${availableToContractQuantitySql} ASC`);
        } else if (order === MarketplaceOrderOrderByInput.availableToContractQuantity_DESC) {
          return Sequelize.literal(`${availableToContractQuantitySql} DESC`);
        } else {
          return order.split('_');
        }
      });
    }

    log.debug('findPage - findOptions:' + JSON.stringify(findOptions));

    const { results, ...paginationData } = await DAL.findAndCountAll(MarketplaceOrderModel, findOptions);
    const pricing = await PricingService.getPricingCapabilitiesMap(results.map(order => order.crop));
    const ordersDTOs = results.map(order => {
      return new MarketplaceOrderDTO(order, pricing[order.crop]);
    });
    return {
      data: ordersDTOs,
      ...paginationData
    };
  }

  // PERFECTEXAMPLE_END

  public static async loadMarketplaceOrdersByUid(ids: string[]) {
    return MarketplaceOrderService.findAll({ ids });
  }

  public static async getGrowerOffersForDemand(
    context: Context,
    where: GrowerOffersForDemandWhereInput,
    sortField: BidSort = BidSort.NEWEST,
    sortDirection: SortDirection = SortDirection.DESC,
    limit: number,
    offset: number
  ) {
    try {
      const connector = new MarketplaceBidAPIConnector({
        customEndpoint: Config.getString('IA_MARKETPLACE_CUSTOM_BID_ENDPOINT')
      });

      const result = await connector.getGrowerOffersForDemand(context, where, sortField, sortDirection, limit, offset);
      const { growerOffers: bidApiGrowerOffers, ...pagingInfo } = result.growerOffersForDemand;

      const marketplaceApiGrowerOffers = bidApiGrowerOffers.map(bidApiOffer => {
        const { supplySource, ...restOfAttributes } = bidApiOffer;
        return new GrowerOfferForDemandImpl({
          id: supplySource.growerOfferId,
          ...restOfAttributes
        });
      });

      return new GrowerOffersForDemandPagedResponseDTO(
        pagingInfo.total,
        pagingInfo.hasNextPage,
        pagingInfo.hasPreviousPage,
        marketplaceApiGrowerOffers
      );
    } catch (err) {
      const msg = `Error calling MarketplaceBidAPIConnector.getGrowerOffersForDemand with params: ${JSON.stringify({
        limit,
        offset,
        sortDirection,
        sortField,
        where
      })}. Error: ${err.message}`;
      log.warn(msg);
      throw new Error(msg);
    }
  }

  public static async closeGrowerMarketplaceOrder(
    uid: string,
    input: GrowerOfferUpdatableAttributes,
    context: Context
  ) {
    const existingOrder = await MarketplaceOrderDAL.findByUid(uid, {
      include: [AddressModel, MarketplaceSupplyProfileModel]
    });
    const isUserGma = await GMAService.isContextUserGma(context);

    const validationError = this.validateOfferForClosing(uid, existingOrder);
    if (validationError) {
      log.warn(validationError.message);

      // Return what we already have if the offer is already closed.
      if (validationError.code === MarketplaceOrderOperationErrorCode.ALREADY_CLOSED) {
        let pricing = null;
        if (existingOrder.isBasisOffer()) {
          pricing = (await PricingService.getPricingCapabilities([existingOrder.crop]))[0];
        }
        return new MarketplaceOrderDTO(existingOrder, pricing);
      }

      throw new Error(validationError.message);
    }

    const [supplyProfile] = await MarketplaceSupplyProfileDAL.findAll({
      internalIds: [existingOrder.supplyProfileId]
    });
    const account = await MarketplaceOrderService.getAccount(context, supplyProfile.accountUid);
    const dto = await MarketplaceOrderService.updateGrowerMarketplaceOrder(existingOrder, input);
    try {
      await MarketplaceOrderNotificationService.sendConfirmationEmail(
        MarketplaceOrderMessageType.Closed,
        existingOrder,
        account,
        isUserGma,
        context
      );
    } catch (err) {
      const msg = `Marketplace order id: ${
        existingOrder.uid
      } was closed but a confirmation email failed to send with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }

    return dto;
  }

  public static async closeExpired(user?: User) {
    const { affectedCount, affectedOrders } = await MarketplaceOrderDAL.closeExpired(user);

    // Poor man's feature flag
    const emailService = new EmailService();

    await Promise.all(
      affectedOrders.map(async order => {
        try {
          const { accountUid } = (await order.$get('supplyProfile')) as MarketplaceSupplyProfileModel;

          const expiresAtString = dateTimeString(order.expiresAt);
          const emailData = {
            crop: cropLabel(order.crop),
            cropQuantity: numberWithCommas(order.cropQuantity),
            cropQuantityUnit: toTitleCase(order.cropQuantityUnit),
            deliveryMethod: joinWithOr(
              order.supportedShippingProviders.map(provider => toTitleCase(ShippingProvider[provider]))
            ),
            expiresAt:
              order.expirationType === MarketplaceOrderExpirationType.EXPIRATION_DATE
                ? expiresAtString
                : `Good til cancel (${expiresAtString})`,
            price: order.isBasisOffer()
              ? getPriceValue(order.basisValue, order.basisMonthCode, order.basisYear)
              : getCommaSeparatedDollars(order.cashPrice),
            pricingType: order.isBasisOffer() ? 'Basis' : 'Cash',
            referenceId: order.referenceId
          };

          const emailContent = emailService.getOfferExpiredEmailToGrower(emailData);
          emailService.sendEmailsToAccountEdgeHighVolume(accountUid, emailContent);
        } catch (err) {
          ErrorReporter.capture(
            `Error sending email about offer expiration. Offer data: ${JSON.stringify(order)}`,
            err
          );
        }
      })
    );

    return affectedCount;
  }

  public static async closeAndCreateNewGrowerMarketplaceOrder(
    input: CreateGrowerMarketplaceOrderInput,
    context: Context,
    product: MarketOrderProduct,
    closeGrowerMarketplaceOrderUid: string
  ) {
    return MarketplaceOrderService.growerMarketplaceOrderCreationHelper(
      input,
      context,
      product,
      closeGrowerMarketplaceOrderUid
    );
  }

  public static async createGrowerMarketplaceOrder(input: CreateGrowerMarketplaceOrderInput, context: Context) {
    return MarketplaceOrderService.growerMarketplaceOrderCreationHelper(input, context, MarketOrderProduct.OTC);
  }

  public static async updateGrowerMarketplaceOrder(
    existingOrder: MarketplaceOrderModel,
    orderUpdates: GrowerOfferUpdatableAttributes
  ) {
    let pricing = null;

    if (orderUpdates.status === MarketOrderStatus.ACCEPTED && existingOrder.isClosed()) {
      const msg = `Marketplace order ${
        existingOrder.uid
      } cannot be accepted because it was closed on ${existingOrder.closedAt.toString()}.`;
      log.warn(msg);
      throw new Error(msg);
    }

    const result = await MarketplaceOrderDAL.updateGrowerOffer(existingOrder.uid, orderUpdates);

    if (result.isBasisOffer()) {
      pricing = (await PricingService.getPricingCapabilities([result.crop]))[0];
    }
    return new MarketplaceOrderDTO(result, pricing);
  }

  public static async createHedgeOrder(input: CreateHedgeOrderInput, context: Context): Promise<MarketplaceOrderDTO> {
    const supplyProfile = await MarketplaceSupplyProfileDAL.find(input.supplyProfileId);
    if (!supplyProfile) {
      throw new Error(
        `Could not create new Hedge Marketplace order; could not find supply profile with ID: ${input.supplyProfileId}`
      );
    }

    const productionLocation = (await ProductionLocationDAL.findAll({ supplyProfileIds: [supplyProfile.id] }))[0];
    if (!productionLocation) {
      throw new Error(
        `Could not create new Hedge Marketplace order; could not find production location for supply profile with ID: ${
          supplyProfile.id
        }`
      );
    }

    const account = await MarketplaceOrderService.getAccount(context, supplyProfile.accountUid);
    const accountName = this.getAccountName(account, input.isAnonymous);

    const creationUser = context.user ? context.user.id : null;
    let newMarketplaceOrderModel: MarketplaceOrderModel = await this.convertCreateHedgeOrderInputToModel(
      input,
      accountName,
      creationUser,
      supplyProfile
    );

    const expiresAtValidationError = this.validateExpiresAt(newMarketplaceOrderModel);
    if (expiresAtValidationError) {
      log.error(expiresAtValidationError.message);
      throw new Error(expiresAtValidationError.message);
    }

    const pricingValidationError = this.validateFutureReferencePrice(newMarketplaceOrderModel);
    if (pricingValidationError) {
      log.error(pricingValidationError.message);
      throw new Error(pricingValidationError.message);
    }

    // Validate that the latest MSA was signed
    await MarketplaceUserAgreementAcceptanceService.verifyLatestUserAgreementSigned(
      context,
      supplyProfile,
      productionLocation
    );

    // Validation passed! Perform appropriate database operations to manipulate orders
    const pickupAddress: Address = input.pickupAddress as Address;

    newMarketplaceOrderModel = await MarketplaceOrderDAL.create(newMarketplaceOrderModel, pickupAddress);

    const isUserGma = await GMAService.isContextUserGma(context);

    try {
      await MarketplaceOrderNotificationService.sendConfirmationEmail(
        MarketplaceOrderMessageType.Created,
        newMarketplaceOrderModel,
        account,
        isUserGma,
        context
      );
    } catch (err) {
      const msg = `Marketplace order was created with id: ${
        newMarketplaceOrderModel.uid
      } but a confirmation email failed to send with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }

    const pricing = (await PricingService.getPricingCapabilities([newMarketplaceOrderModel.crop]))[0];

    return new MarketplaceOrderDTO(newMarketplaceOrderModel, pricing);
  }

  public static async fillHedgeOrder(input: FillHedgeOrderInput, context: Context): Promise<MarketplaceOrderDTO> {
    const existingOrder: MarketplaceOrderModel = await MarketplaceOrderDAL.findByUid(input.marketplaceOrderId);

    const fillValidationError = this.validateHedgeOrderForFilling(input, existingOrder);
    if (fillValidationError) {
      log.warn(fillValidationError.message);
      throw new Error(fillValidationError.message);
    }

    const orderUpdates: HedgeOfferUpdatableAttributes = {
      filledQuantity: input.filledQuantity,
      status: MarketOrderStatus.FILLED
    };

    // Futures reference price may or may not be updated to the final price of the order at this stage.
    if (input.filledFuturesReferencePriceValue) {
      orderUpdates.futuresReferencePrice = input.filledFuturesReferencePriceValue;
    }

    const [result] = await DatabaseConnection.transact(transaction => {
      return Promise.all([
        MarketplaceOrderDAL.updateHedgeOrder(existingOrder.uid, orderUpdates, transaction),
        OrderFeeDAL.createOrderFee(
          {
            fee: input.hedgeFee,
            feeType: OrderFeeType.HEDGE,
            marketplaceOrderId: existingOrder.id,
            payor: OrderFeePayor.SELLER
          },
          context,
          transaction
        )
      ]);
    });

    MarketplaceOrderNotificationService.sendFillHedgeOrderNotification(context, result);

    const pricing = (await PricingService.getPricingCapabilities([result.crop]))[0];
    return new MarketplaceOrderDTO(result, pricing);
  }

  /* Helper methods */

  static async convertCreateHedgeOrderInputToModel(
    input: CreateHedgeOrderInput,
    accountName: string,
    createdById: string,
    supplyProfile?: MarketplaceSupplyProfileModel
  ): Promise<MarketplaceOrderModel> {
    const { supplyProfileId: supplyProfileUid, price, pickupAddress, isAnonymous, cropQuality, ...restOfInput } = input;

    const activeSupplyProfile = supplyProfile || (await MarketplaceSupplyProfileDAL.find(supplyProfileUid));
    if (!activeSupplyProfile) {
      const msg =
        // Prettier formats this past the 120 line length limit w/o this split
        `Unable to create hedge marketplace order. ` +
        `Could not find Marketplace Supply Profile with ID: ${supplyProfileUid}`;
      log.error(msg);
      throw new Error(msg);
    }

    let model = new MarketplaceOrderModel({ ...restOfInput });
    model.product = MarketOrderProduct.HEDGE;
    model.type = input.type;
    model.accountName = accountName;
    model.createdBy = createdById;
    model.supplyProfileId = activeSupplyProfile.id;
    model.cropQuality = cropQuality as CropQualitySpecification;

    model.basisMonthCode = price.month as FuturesMonthCode;
    model.futuresReferencePrice = price.value;
    model.basisYear = price.year;
    model.cashPriceCurrencyCode = price.currencyCode;

    model = this.setDefaultValuesOnMarketplaceOrderModel(model);

    return model;
  }

  static async convertCreateGrowerMarketplaceOrderInputToModel(
    input: CreateGrowerMarketplaceOrderInput,
    accountName: string,
    createdById: string,
    originatingDemandOrderId: string,
    supplyProfile?: MarketplaceSupplyProfileModel
  ): Promise<MarketplaceOrderModel> {
    const { supplyProfileId: supplyProfileUid, price, pickupAddress, isAnonymous, cropQuality, ...restOfInput } = input;

    const activeSupplyProfile = supplyProfile || (await MarketplaceSupplyProfileDAL.find(supplyProfileUid));
    if (!activeSupplyProfile) {
      const msg =
        // Prettier formats this past the 120 line length limit w/o this split
        `Unable to create marketplace order. ` +
        `Could not find Marketplace Supply Profile with ID: ${supplyProfileUid}`;
      log.error(msg);
      throw new Error(msg);
    }

    let model = new MarketplaceOrderModel({ ...restOfInput });
    model.originatingDemandOrderId = originatingDemandOrderId;
    model.product = MarketOrderProduct.OTC;
    model.type = input.type;
    model.accountName = accountName;
    model.createdBy = createdById;
    model.supplyProfileId = activeSupplyProfile.id;
    model.cropQuality = cropQuality as CropQualitySpecification;

    if (price.cashInput) {
      model.cashPrice = price.cashInput.value;
      model.cashPriceCurrencyCode = price.cashInput.currencyCode;
    }
    if (price.basisInput) {
      model.basisMonthCode = price.basisInput.month as FuturesMonthCode;
      model.basisValue = price.basisInput.value;
      model.basisYear = price.basisInput.year;
    }
    model = this.setDefaultValuesOnMarketplaceOrderModel(model);

    return model;
  }

  static setDefaultValuesOnMarketplaceOrderModel(model: MarketplaceOrderModel): MarketplaceOrderModel {
    model.product = model.product || MarketOrderProduct.OTC;
    model.type = model.type || MarketOrderType.SELL;
    model.status = MarketOrderStatus.WORKING;
    model.filledQuantity = 0;
    model.contractedQuantity = 0;

    return model;
  }

  static getAccountName(account: Account, isAnonymous: boolean): string {
    let useAnonymous = isAnonymous;
    if (isAnonymous !== false) {
      useAnonymous = true;
    }
    const accountName = useAnonymous ? null : account.name;
    return accountName;
  }

  static validateHedgeOrderForFilling(
    input: FillHedgeOrderInput,
    order: MarketplaceOrderModel
  ): MarketplaceOrderOperationError | void {
    if (!order) {
      return {
        code: MarketplaceOrderOperationErrorCode.NOT_FOUND,
        message: `Cannot find MarketplaceOrder with id: ${input.marketplaceOrderId}. Fill hedge failed.`
      };
    }
    if (order.product !== MarketOrderProduct.HEDGE) {
      return {
        code: MarketplaceOrderOperationErrorCode.NOT_HEDGE_ORDER,
        message: `Marketplace order ${input.marketplaceOrderId} cannot be filled because it is not a hedge order.`
      };
    }
    if (order.status === MarketOrderStatus.FILLED) {
      return {
        code: MarketplaceOrderOperationErrorCode.ALREADY_FILLED,
        message: `Marketplace order ${input.marketplaceOrderId} is already filled.`
      };
    }
    if (order.status !== MarketOrderStatus.WORKING) {
      return {
        code: MarketplaceOrderOperationErrorCode.INVALID_STATUS_FOR_OPERATION,
        message: `Marketplace order ${input.marketplaceOrderId} is not in WORKING status but is ${order.status}.`
      };
    }

    if (input.filledQuantity > order.cropQuantity) {
      return {
        code: MarketplaceOrderOperationErrorCode.CANNOT_FILL_OVER_CONTRACT,
        message: `Marketplace order ${input.marketplaceOrderId} is cannot be filled for ${
          input.filledQuantity
        } over the cropQuantity ${order.cropQuantity}.`
      };
    }

    if (input.hedgeFee.value < 0) {
      return {
        code: MarketplaceOrderOperationErrorCode.INVALID_HEDGE_FEE,
        message: `Hedge fee for filling order ${input.marketplaceOrderId} is invalid: ${input.hedgeFee}`
      };
    }
  }

  static validateOfferForClosing(uid: string, order: MarketplaceOrderModel): MarketplaceOrderOperationError | void {
    if (!order) {
      return {
        code: MarketplaceOrderOperationErrorCode.NOT_FOUND,
        message: `Cannot find MarketplaceOrder with id: ${uid}.  Update failed.`
      };
    }

    // Can't close accepted offers
    if (order.isAccepted()) {
      return {
        code: MarketplaceOrderOperationErrorCode.CANNOT_CLOSE_ACCEPTED,
        message: `Marketplace order ${uid} cannot be closed because it has been accepted.`
      };
    }

    if (order.isClosed()) {
      return {
        code: MarketplaceOrderOperationErrorCode.ALREADY_CLOSED,
        message: `Marketplace order ${uid} is already closed.`
      };
    }
  }

  static async growerMarketplaceOrderCreationHelper(
    input: CreateGrowerMarketplaceOrderInput,
    context: Context,
    product?: MarketOrderProduct,
    closeGrowerMarketplaceOrderUid?: string
  ): Promise<MarketplaceOrderDTO> {
    if (closeGrowerMarketplaceOrderUid) {
      // Validate closing of existing order
      const existingOrder = await MarketplaceOrderDAL.findByUid(closeGrowerMarketplaceOrderUid);

      const closeValidationError = this.validateOfferForClosing(closeGrowerMarketplaceOrderUid, existingOrder);
      if (closeValidationError) {
        log.warn(closeValidationError.message);
        throw new Error(closeValidationError.message);
      }
    }

    let originatingDemandOrderId: string;
    if (input.originatingDemandOrderId) {
      const originatingDemandOrder = await DemandOrderModel.findOne({ where: { uid: input.originatingDemandOrderId } });
      if (!originatingDemandOrder) {
        throw new NestedError(`Failed to find originating demand order`, {
          originatingDemandOrderId: input.originatingDemandOrderId
        });
      }

      originatingDemandOrderId = originatingDemandOrder.id;
    }

    // Validate new order input
    const supplyProfile = await MarketplaceSupplyProfileDAL.find(input.supplyProfileId);
    if (!supplyProfile) {
      throw new Error(
        `Could not create new Marketplace order; could not find supply profile with ID: ${input.supplyProfileId}`
      );
    }

    const productionLocation = (await ProductionLocationDAL.findAll({ supplyProfileIds: [supplyProfile.id] }))[0];
    if (!productionLocation) {
      throw new Error(
        `Could not create new Marketplace order; could not find production location for supply profile with ID: ${
          supplyProfile.id
        }`
      );
    }

    const account = await MarketplaceOrderService.getAccount(context, supplyProfile.accountUid);
    const accountName = this.getAccountName(account, input.isAnonymous);
    const creationUser = context.user ? context.user.id : null;

    let newMarketplaceOrderModel: MarketplaceOrderModel = await this.convertCreateGrowerMarketplaceOrderInputToModel(
      input,
      accountName,
      creationUser,
      originatingDemandOrderId,
      supplyProfile
    );

    const pricingDetails = await this.getPricingFromCreateInput(input);

    const pricingValidationError = this.validateOTCPricing(input, pricingDetails);
    if (pricingValidationError) {
      log.error(pricingValidationError.message);
      throw new Error(pricingValidationError.message);
    }

    const deliveryDateValidationError = this.validateDeliveryDate(newMarketplaceOrderModel);
    if (deliveryDateValidationError) {
      log.error(deliveryDateValidationError.message);
      throw new Error(deliveryDateValidationError.message);
    }

    const expiresAtValidationError = this.validateExpiresAt(newMarketplaceOrderModel);
    if (expiresAtValidationError) {
      log.error(expiresAtValidationError.message);
      throw new Error(expiresAtValidationError.message);
    }

    // Validate that the latest MSA was signed
    await MarketplaceUserAgreementAcceptanceService.verifyLatestUserAgreementSigned(
      context,
      supplyProfile,
      productionLocation
    );

    // Validation passed! Perform appropriate database operations to manipulate orders

    const pickupAddress: Address = input.pickupAddress as Address;

    if (closeGrowerMarketplaceOrderUid) {
      newMarketplaceOrderModel = await MarketplaceOrderDAL.closeAndCreateNewGrowerMarketplaceOrder(
        newMarketplaceOrderModel,
        pickupAddress,
        accountName,
        creationUser,
        closeGrowerMarketplaceOrderUid
      );
    } else {
      newMarketplaceOrderModel = await MarketplaceOrderDAL.create(newMarketplaceOrderModel, pickupAddress);
    }

    const isUserGma = await GMAService.isContextUserGma(context);

    try {
      await MarketplaceOrderNotificationService.sendConfirmationEmail(
        closeGrowerMarketplaceOrderUid ? MarketplaceOrderMessageType.Updated : MarketplaceOrderMessageType.Created,
        newMarketplaceOrderModel,
        account,
        isUserGma,
        context
      );
    } catch (err) {
      const msg = `Marketplace order was created with id: ${
        newMarketplaceOrderModel.uid
      } but a confirmation email failed to send with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }

    return new MarketplaceOrderDTO(newMarketplaceOrderModel, pricingDetails.newOrderPricing);
  }

  static validateOTCPricing(
    input: CreateGrowerMarketplaceOrderInput,
    pricingDetails: CreateInputPricingDetails
  ): MarketplaceOrderOperationError | void {
    if (!isNil(input.price.basisInput) && !isNil(input.price.cashInput)) {
      return {
        code: MarketplaceOrderOperationErrorCode.CASH_AND_BASIS_FORBIDDEN,
        message: `Cannot create a marketplace order with cash and basis data: ${JSON.stringify(input)}`
      };
    }

    if (!input.price.basisInput && !input.price.cashInput) {
      return {
        code: MarketplaceOrderOperationErrorCode.CASH_OR_BASIS_REQUIRED,
        message: `Cannot create a marketplace order with neither cash nor basis data: ${JSON.stringify(input)}`
      };
    }

    if (pricingDetails.isBasisOffer) {
      if (
        !pricingDetails.newOrderPricing ||
        !_.includes(pricingDetails.newOrderPricing.supportedPricingTypes, CropPricingType.BASIS)
      ) {
        return {
          code: MarketplaceOrderOperationErrorCode.BASIS_NOT_SUPPORTED_FOR_CROP,
          message: `Cannot create marketplace order.  Basis pricing is not supported for crop ${
            input.crop
          }. ${JSON.stringify(input)}`
        };
      }
    } else {
      if (!isNil(input.price.cashInput) && input.price.cashInput.value <= 0) {
        return {
          code: MarketplaceOrderOperationErrorCode.NON_POSITIVE_CASH_VALUE,
          message: `Cannot create a marketplace order with negative or 0 cash value: ${JSON.stringify(input)}`
        };
      }
    }
  }

  static validateDeliveryDate(model: MarketplaceOrderModel): MarketplaceOrderOperationError | void {
    // delivery date validation
    const endDate: Date = model.deliveryWindowEndAt;
    const startDate: Date = model.deliveryWindowStartAt;
    if (endDate < startDate) {
      return {
        code: MarketplaceOrderOperationErrorCode.DELIVERY_END_BEFORE_DELIVERY_START,
        message:
          `Cannot create a marketplace order with a delivery end date: ${JSON.stringify(endDate)} before` +
          ` the delivery start date: ${JSON.stringify(startDate)}`
      };
    }
  }

  static validateExpiresAt(model: MarketplaceOrderModel): MarketplaceOrderOperationError | void {
    // expiresAt validation
    const expiresAt: Date = model.expiresAt;
    if (expiresAt <= new Date()) {
      return {
        code: MarketplaceOrderOperationErrorCode.EXPIRES_AT_IN_THE_PAST,
        message: `Cannot create a marketplace order with an expiresAt in the past ${JSON.stringify(model.expiresAt)}`
      };
    }
  }

  static validateFutureReferencePrice(model: MarketplaceOrderModel): MarketplaceOrderOperationError | void {
    if (model.futuresReferencePrice <= 0 || isNil(model.basisMonthCode) || isNil(model.basisYear)) {
      return {
        code: MarketplaceOrderOperationErrorCode.INVALID_FUTURE_REFERENCE_PRICE,
        message:
          `Cannot create a hedge marketplace order without a valid future reference price: ` +
          `${model.futuresReferencePrice}, basisMonthCode: ${model.basisMonthCode}, and basisYear: ${model.basisYear}`
      };
    }
  }

  private static async getPricingFromCreateInput(
    input: CreateGrowerMarketplaceOrderInput
  ): Promise<CreateInputPricingDetails> {
    const isBasisOffer = !!(input.price && input.price.basisInput);
    const newOrderPricing = isBasisOffer ? (await PricingService.getPricingCapabilities([input.crop]))[0] : null;
    return { isBasisOffer, newOrderPricing };
  }

  private static async getAccount(context: Context, accountId: string) {
    const account = await UserService.getAccount(context, accountId, `{ id name owner { id email } }`);
    if (!account) {
      throw new TypeNotFoundError(`Cannot create MarketplaceOrder for non-existent account ${accountId}`);
    }
    return account;
  }
}
