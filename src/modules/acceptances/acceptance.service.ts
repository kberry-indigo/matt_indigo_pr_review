import {
  AcceptBidInput,
  AcceptGrowerOfferInput,
  AssignBasisToHedgeOrderInput,
  BidType,
  DemandOrderStatus,
  GeoPointInput,
  GrowerOpportunityAcceptInput,
  LimitOffsetPaginationInput,
  MarketOrderProduct,
  MarketOrderStatus,
  MarketplaceAcceptanceInitiatedBy,
  MarketplaceAcceptanceLockBasisPriceInput,
  MarketplaceAcceptanceOrderByInput,
  MarketplaceAcceptanceWhereInput,
  MarketplaceOrderExpirationType,
  ShippingProvider,
  User
} from '@indigo-ag/schema';
import {
  AccountUserManagementConnector,
  Context,
  ErrorReporter,
  NestedError,
  RoleNames,
  TypeNotFoundError
} from '@indigo-ag/server';
import { UserInputError } from 'apollo-server-errors';
import { Transaction, WhereOptions } from 'sequelize';
import { IFindOptions, IIncludeOptions } from 'sequelize-typescript';
import { BidAPIBid } from '../../connectors/bid_api/connector';
import { BidAPIBenchmarkPriceInput, BidForTargetInput } from '../../connectors/bid_api/inputs';
import { Config } from '../../core/config';
import { DAL } from '../../core/dal';
import { DatabaseConnection } from '../../core/sequelize';
import {
  EmailService,
  GeoService,
  MarketplaceOrderAcceptanceEmailData,
  SendBidAcceptanceForGrower,
  SendPriceLockConfirmationData,
  UserService
} from '../../services';
import {
  basisLockInPriceDeadline,
  cropLabel,
  cropQuality,
  dateRange,
  dateTimeString,
  getCommaSeparatedDollars,
  getDistanceDisplay,
  getPriceValue,
  numberWithCommas,
  phoneNumber as formatPhoneNumber,
  toTitleCase
} from '../../services/format';
import { AddressDAL } from '../addresses';
import AddressModel from '../addresses/address.model';
import { MarketplaceDemandProfileModel, MarketplaceDemandProfileService } from '../demand';
import { MarketplaceDemandProfileDAL } from '../demand/profile/profile.dal';
import { MarketplaceOrderDAL, MarketplaceOrderModel } from '../marketplaceOrder';
import { BidAPIDAL } from '../matches/rtbm/dal/bid_api/bid.dal';
import { AcceptanceFeeService } from '../orderFees/acceptanceFee.service';
import { DemandOrderModel, OrderDAL, OrderService } from '../orders';
import { createBasisPrice, PricingService } from '../pricing';
import { PricingCapabilitiesDTO } from '../pricing/pricing.dto';
import {
  MarketplaceSupplyProfileDAL,
  MarketplaceSupplyProfileModel,
  MarketplaceSupplyProfileService,
  ProductionLocationDAL,
  ProductionLocationModel,
  ProductionLocationService
} from '../supply';
import { GMAService, SaleType } from '../supply/gma.service';
import { MarketplaceUserAgreementAcceptanceService } from '../userAgreementAcceptance';
import { BidAcceptanceDAL, BidAcceptanceWhere } from './acceptance.dal';
import { BidAcceptanceDTO } from './acceptance.dto';
import MarketplaceAcceptanceModel from './acceptance.model';

export type AcceptanceCreateFromMatchInput = GrowerOpportunityAcceptInput & { isBasis: boolean };
const PRICE_DECIMAL_PLACES = 5;

export class BidAcceptanceService {
  public static async findByMarketOrderIds(ids: number[]) {
    const acceptances = await BidAcceptanceDAL.findAll({ marketplaceOrderIds: ids });
    const pricing = await PricingService.getPricingCapabilitiesMap(acceptances.map(a => a.crop));
    return acceptances.map(model => new BidAcceptanceDTO(model, pricing[model.crop]));
  }

  public static async findAll(where: BidAcceptanceWhere) {
    if (where.demandOrderId) {
      // replace uid with id of demandOrder
      const demandOrder = await OrderDAL.findByUid(where.demandOrderId);
      if (!demandOrder) {
        throw new Error(`Cannot find acceptances for demandOrder ${where.demandOrderId}`);
      }
      where.demandOrderId = demandOrder.id;
    }
    const acceptanceModels = await BidAcceptanceDAL.findAll(where);
    if (!acceptanceModels) {
      return [];
    }
    const pricing = await PricingService.getPricingCapabilitiesMap(acceptanceModels.map(a => a.crop));
    return acceptanceModels.map(model => new BidAcceptanceDTO(model, pricing[model.crop]));
  }

  public static async lockAcceptanceBasisPrice(input: MarketplaceAcceptanceLockBasisPriceInput, context?: Context) {
    log.info(`Locking price of ${input.price} for acceptance ${input.acceptanceId}`);
    return DatabaseConnection.transact<BidAcceptanceDTO>(async function(transaction) {
      const acceptanceModel = await BidAcceptanceService.findById(input.acceptanceId);
      if (!acceptanceModel) {
        throw new Error(`Cannot lock in basis price. No acceptance found with id ${input.acceptanceId}`);
      }

      if (!acceptanceModel.demandOrderId) {
        throw new Error(
          `Cannot lock in basis price, no demand order found associated to acceptance id ${input.acceptanceId}`
        );
      }

      if (typeof acceptanceModel.basisLockedInPrice === 'number') {
        throw new Error(
          `Cannot lock in basis price for acceptance that is already locked.  Acceptance ID: ${input.acceptanceId}`
        );
      }

      const updateObj = { basisLockedInPrice: input.price, priceLockedDate: new Date() };
      const acceptanceUpdate = await BidAcceptanceDAL.update(input.acceptanceId, updateObj, { transaction });
      if (!acceptanceUpdate) {
        throw new Error(
          `Failed to update the acceptance with id ${
            input.acceptanceId
          }, could not lock in price with info ${JSON.stringify(input)}`
        );
      }
      log.info(`Price of ${input.price} for acceptance ${input.acceptanceId} is now locked in.`);

      // Send out confirmation email to grower entities.
      try {
        await BidAcceptanceService.sendLockAcceptanceBasisPriceEmail(
          context,
          acceptanceUpdate,
          acceptanceModel.acceptedByUid,
          acceptanceModel.id
        );
      } catch (e) {
        const msg = `Error occurred while sending futures price lock email.`;
        log.error(msg, e);
        ErrorReporter.capture(msg, e);
      }

      log.info(`Lock in email to grower for acceptance ${input.acceptanceId}.`);
      const pricing = await PricingService.getPricingCapabilities([acceptanceModel.crop]);
      return new BidAcceptanceDTO(acceptanceUpdate, pricing[0]);
    });
  }

  static async sendLockAcceptanceBasisPriceEmail(
    context: Context,
    acceptanceUpdate: MarketplaceAcceptanceModel,
    acceptedByUid: string,
    acceptanceId: string
  ) {
    const growerSupplyProfileQuery = { id: acceptanceUpdate.sellerUid };
    const growerSupplyProfile = await MarketplaceSupplyProfileService.find(growerSupplyProfileQuery);
    const growerAccount = await UserService.getAccount(
      context,
      growerSupplyProfile.accountId,
      `{ id name owner { id email } }`
    );
    const cashPrice = acceptanceUpdate.buyerBasisValue + acceptanceUpdate.basisLockedInPrice;
    const emailService = new EmailService();
    const connectedGmaUser = await GMAService.getAssociatedGMAUser(growerSupplyProfile.accountId, context);
    const originatingUser = await BidAcceptanceService.getOriginatingUser(context, acceptedByUid);
    if (!originatingUser) {
      throw Error(`Unable to find seller user for ID ${acceptedByUid} to send grower offer acceptance email.`);
    }
    const genericEmailData: SendPriceLockConfirmationData = {
      basisPrice: getPriceValue(
        acceptanceUpdate.buyerBasisValue,
        acceptanceUpdate.basisMonthCode,
        acceptanceUpdate.basisYear
      ),
      bushelsSold: numberWithCommas(acceptanceUpdate.cropQuantity),
      cashPrice: getCommaSeparatedDollars(cashPrice),
      connectedGmaEmail: connectedGmaUser ? connectedGmaUser.email : null,
      crop: cropLabel(acceptanceUpdate.crop),
      growerEmail: growerAccount.owner.email,
      isSellerGma: GMAService.userHasGmaPermissions(originatingUser.permissions),
      lockedPrice: getCommaSeparatedDollars(acceptanceUpdate.basisLockedInPrice),
      priceLockedDate: dateTimeString(acceptanceUpdate.priceLockedDate),
      saleType: SaleType.SELLER_INITIATED,
      sellerActingCapacity: UserService.getSellerActingCapacity(context.user.id, originatingUser.id),
      sellerEmail: originatingUser.email
    };
    log.info(
      `Sending lock in email to grower for acceptance ${acceptanceId} with info :${JSON.stringify(genericEmailData)}`
    );

    const emailFunc = EmailService.prototype.sendFuturesReferencePriceConfirmation;
    const sentTo: string[] = await emailService.sendEmailsToAccountEdge<SendPriceLockConfirmationData>(
      genericEmailData,
      growerSupplyProfile.accountId,
      context,
      emailFunc
    );

    await GMAService.handleGmaGrowerEmail(
      genericEmailData,
      growerSupplyProfile.accountId,
      emailService,
      sentTo,
      emailFunc
    );
  }

  public static async assignBasisToHedgeOrder(input: AssignBasisToHedgeOrderInput, context: Context) {
    log.info(
      `assigning demandOrderId ${input.demandOrderId} to marketplaceOrderId ${input.marketplaceOrderId} for ${
        input.quantity
      } bushels`
    );
    const acceptance = await DatabaseConnection.transact(async transaction => {
      // Retrieve demandOrder and marketplaceOrder
      const [demandOrder, marketplaceOrder, supplyProfile] = await Promise.all([
        OrderDAL.findByUid(input.demandOrderId, transaction),
        MarketplaceOrderDAL.findByUid(input.marketplaceOrderId, { transaction }),
        MarketplaceSupplyProfileDAL.find(input.sellerId)
      ]);

      this.validateAssignBasisToHedgeInput(demandOrder, marketplaceOrder, supplyProfile, input);

      await this.verifyOwnerOrFullGMA(context, supplyProfile);

      const productionLocations = await ProductionLocationDAL.findAll({ supplyProfileIds: [supplyProfile.id] });
      if (!productionLocations || productionLocations.length < 1) {
        throw new TypeNotFoundError(`Could not find production locations for supply profile ${supplyProfile.id}`);
      }
      // For now, one productionLocation per supply profile is expected.
      const bidLocation = ProductionLocationService.coordinatesFromProductionLocation(productionLocations[0]);

      const bid = await this.getBid(demandOrder, input, bidLocation, context);

      this.validateAssignBasisToHedgePricing(demandOrder, marketplaceOrder, input, bid);

      await Promise.all([
        OrderDAL.updateAcceptedQuantity(input.demandOrderId, input.quantity, { transaction }),
        MarketplaceOrderDAL.incrementContractedQuantity(input.marketplaceOrderId, input.quantity, { transaction })
      ]);

      const acceptanceModel = await this.createAcceptanceWithAddresses(
        context,
        input,
        bid,
        demandOrder,
        productionLocations[0],
        transaction,
        marketplaceOrder.id,
        // FRP is carried over from the order and stored in the "basisLockedInPrice" column on the acceptance
        marketplaceOrder.futuresReferencePrice
      );
      await AcceptanceFeeService.applyOrderFeesToAcceptance(marketplaceOrder.id, acceptanceModel.id, transaction);
      return acceptanceModel;
    });
    const [pricing] = await PricingService.getPricingCapabilities([acceptance.crop]);
    return new BidAcceptanceDTO(acceptance, pricing);
  }

  public static async acceptGrowerOffer(context: Context, input: AcceptGrowerOfferInput) {
    const marketplaceOrder = await MarketplaceOrderDAL.findByUid(input.marketplaceOrderId, {
      include: [AddressModel, MarketplaceSupplyProfileModel]
    });

    if (!marketplaceOrder) {
      throw new Error(`Could not accept grower offer.  Offer for id: ${input.marketplaceOrderId} not found.`);
    }

    if (marketplaceOrder.product !== MarketOrderProduct.OTC) {
      throw new Error(
        `Could not accept grower offer.  Order is not an OTC order.  Order Product: ${marketplaceOrder.product}`
      );
    }

    if (!marketplaceOrder.isOpen()) {
      throw new Error(`Could not accept grower offer.  Order is not open.  Order Status: ${marketplaceOrder.status}`);
    }

    if (marketplaceOrder.isExpired()) {
      throw new Error(
        `Could not accept an expired grower offer.  Order expired on: ${marketplaceOrder.expiresAt.toISOString()}`
      );
    }

    const buyerProfile = await MarketplaceDemandProfileDAL.find(input.buyerId);
    if (!buyerProfile) {
      throw new TypeNotFoundError(`Could not accept grower offer.  Demand profile for id: ${input.buyerId} not found.`);
    }

    const creatorEmail = context.user && context.user.email;
    const creatorId = context.user && context.user.id;

    // Accept BID does some account verification but acts on the supply profile.  Do we need
    // to do something like that here as well?

    const acceptance = await DatabaseConnection.transact(async transaction => {
      marketplaceOrder.status = MarketOrderStatus.ACCEPTED;

      /*
      For OTC orders we are setting filledQuantity and contractedQuantity the same.
      In the context of floating basis (hedge) orders, these values may/will differ.
      As the filledQuantity represents the portion of the hedge that has been filled on the market
      and contractedQuantity represents the quantity that has a basis accepted and applied against
      that filledQuantity.
       */
      marketplaceOrder.filledQuantity = marketplaceOrder.cropQuantity;
      marketplaceOrder.contractedQuantity = marketplaceOrder.cropQuantity;

      await marketplaceOrder.save({ transaction });

      const deliveryAddress = await AddressDAL.create(
        {
          city: buyerProfile.city,
          country: buyerProfile.country,
          county: buyerProfile.county,
          postalCode: buyerProfile.postalCode,
          state: buyerProfile.state,
          street: buyerProfile.street
        },
        null,
        { transaction }
      );

      const demandOrder = await OrderDAL.create(
        {
          acceptedQuantity: marketplaceOrder.cropQuantity,
          basisMonthCode: marketplaceOrder.basisMonthCode,
          basisValue: marketplaceOrder.basisValue,
          basisYear: marketplaceOrder.basisYear,
          buyerId: input.buyerId,
          buyerType: input.buyerType,
          contact: {
            email: buyerProfile.contactEmail,
            name: buyerProfile.contactName,
            phoneNumber: buyerProfile.contactPhone
          },
          createdBy: 'tech-eng@indigoag.com',
          crop: marketplaceOrder.crop,
          cropQualityConstraint: marketplaceOrder.cropQuality,
          cropQuantity: marketplaceOrder.cropQuantity,
          cropUnit: marketplaceOrder.cropQuantityUnit,
          currencyCode: marketplaceOrder.cashPriceCurrencyCode,
          deliveryAtEnd: marketplaceOrder.deliveryWindowEndAt.toISOString(),
          deliveryAtStart: marketplaceOrder.deliveryWindowStartAt.toISOString(),
          expiresAt: marketplaceOrder.expiresAt.toISOString(),
          growerMaximumFulfillmentQuantity: marketplaceOrder.cropQuantity,
          indigoFee: 0,
          latitude: deliveryAddress.latitude,
          location: deliveryAddress,
          longitude: deliveryAddress.longitude,
          maxRadiusMiles: marketplaceOrder.maxRadiusMiles,
          notesToGrower: `Automatically created to fulfill grower offer ${marketplaceOrder.referenceId}`,
          ownedBy: creatorEmail,
          price: marketplaceOrder.cashPrice,
          shareBuyerInformation: false,
          status: DemandOrderStatus.CLOSED,
          supportedShippingProviders: marketplaceOrder.supportedShippingProviders,
          type: BidType.PARTNER_BID,
          variety: marketplaceOrder.variety
        },
        { transaction }
      );

      const price = marketplaceOrder.cashPrice
        ? { cashInput: { currencyCode: marketplaceOrder.cashPriceCurrencyCode, value: marketplaceOrder.cashPrice } }
        : {
            basisInput: {
              month: marketplaceOrder.basisMonthCode,
              value: marketplaceOrder.basisValue,
              year: marketplaceOrder.basisYear
            }
          };

      const bidInput: AcceptBidInput = {
        demandOrderId: demandOrder.id,
        price,
        quantity: marketplaceOrder.cropQuantity,
        quantityUnit: marketplaceOrder.cropQuantityUnit,
        sellerId: marketplaceOrder.supplyProfile.uid,
        shippingProvider: marketplaceOrder.supportedShippingProviders[0] // Assume 0 for grower offer
      };

      const bid = await this.getBid(
        demandOrder,
        bidInput,
        {
          latitude: marketplaceOrder.address.latitude,
          longitude: marketplaceOrder.address.longitude
        },
        context
      );

      return BidAcceptanceDAL.create(
        bidInput,
        demandOrder,
        bid,
        MarketplaceAcceptanceInitiatedBy.BUYER,
        marketplaceOrder.addressId,
        deliveryAddress.id,
        creatorId,
        context.user ? `${context.user.firstName} ${context.user.lastName}` : null,
        marketplaceOrder.id,
        null,
        { transaction }
      );
    });

    // Send confirmations emails.
    const [supplyProfile] = await MarketplaceSupplyProfileDAL.findAll({
      internalIds: [marketplaceOrder.supplyProfileId]
    });
    try {
      log.info('Sending offer accepted emails');
      await BidAcceptanceService.sendOfferAcceptanceEmail(
        marketplaceOrder,
        acceptance,
        supplyProfile,
        buyerProfile,
        context
      );
    } catch (err) {
      const msg =
        `Marketplace order was accepted with acceptance uid: ${acceptance.uid} but a confirmation email failed ` +
        `to send with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }

    const [pricing] = await PricingService.getPricingCapabilities([acceptance.crop]);
    return new BidAcceptanceDTO(acceptance, pricing);
  }

  public static async acceptBid(context: Context, input: AcceptBidInput) {
    const demandOrder = await OrderDAL.findByUid(input.demandOrderId);
    if (!demandOrder) {
      throw new Error(`Could not accept bid for demand order ${input.demandOrderId}`);
    }

    if (demandOrder.status === DemandOrderStatus.CLOSED) {
      throw new Error(`Could not accept bid for a closed demand order ${input.demandOrderId}`);
    }

    if (demandOrder.acceptedQuantity >= demandOrder.cropQuantity) {
      throw new Error(`Could not accept bid. The order is filled. ${input.demandOrderId}`);
    }

    if (demandOrder.acceptedQuantity + input.quantity > demandOrder.cropQuantity) {
      throw new Error(`Could not accept bid. The input quantity and acceptedQuantity exceed the
      cropQuantity. ${input.demandOrderId}`);
    }

    const supplyProfile = await MarketplaceSupplyProfileDAL.find(input.sellerId);
    if (!supplyProfile) {
      throw new Error(`Could not accept bid for seller ${input.sellerId}`);
    }
    const productionLocation = (await ProductionLocationDAL.findAll({ supplyProfileIds: [supplyProfile.id] }))[0];
    if (!productionLocation) {
      throw new Error(
        `Could not accept bid, could not find production location for supply profile ${supplyProfile.id}`
      );
    }
    // Validate that the acceptance is for the correct pricing type.
    // APBs don't have any pricing, but only accept a basis price.
    if (input.price.cashInput && !_.isNumber(demandOrder.price)) {
      throw new Error(`Cannot accept demandOrder ${demandOrder.id} for basis bid with a cash price`);
    } else if (
      input.price.basisInput &&
      demandOrder.type === BidType.PARTNER_BID &&
      !_.isNumber(demandOrder.basisValue)
    ) {
      throw new Error(`Cannot accept demandOrder ${demandOrder.id} for cash bid with a basis price`);
    }

    await this.verifyOwnerOrFullGMA(context, supplyProfile);

    await MarketplaceUserAgreementAcceptanceService.verifyLatestUserAgreementSigned(
      context,
      supplyProfile,
      productionLocation
    );

    // Validate the acceptance price matches the bid price as generated by the bid-api
    const location = ProductionLocationService.coordinatesFromProductionLocation(productionLocation);
    const bid = await this.getBid(demandOrder, input, location, context);

    if (bid.pricingDetails.__typename === 'CashPrice') {
      if (
        _.round(bid.pricingDetails.value, PRICE_DECIMAL_PLACES) !==
        // Add the indigo fee back in since the bid won't have it removed, but the grower saw a lower price
        _.round(input.price.cashInput.value + demandOrder.indigoFee, PRICE_DECIMAL_PLACES)
      ) {
        // TODO: Return an error code here
        throw new Error(
          `Cash bid price ${bid.pricingDetails.value} does not match given acceptance price ${
            input.price.cashInput.value
          }`
        );
      }
    } else {
      const basisPrice = bid.pricingDetails;
      const inputPrice = input.price.basisInput;
      if (
        basisPrice.month !== inputPrice.month ||
        basisPrice.year !== inputPrice.year ||
        // Add the indigo fee back in since the bid won't have it removed, but the grower saw a lower price
        _.round(basisPrice.value, PRICE_DECIMAL_PLACES) !==
          _.round(inputPrice.value + demandOrder.indigoFee, PRICE_DECIMAL_PLACES)
      ) {
        // TODO: Return an error code here
        throw new Error(
          `Basis bid price ${JSON.stringify(basisPrice)} does not match given acceptance price ${JSON.stringify(
            inputPrice
          )}`
        );
      }
    }

    const acceptance = await DatabaseConnection.transact(async transaction => {
      await OrderService.updateAcceptedQuantity(demandOrder.uid, input.quantity, { transaction });
      return this.createAcceptanceWithAddresses(context, input, bid, demandOrder, productionLocation, transaction);
    });

    if (acceptance) {
      const [pricing] = await PricingService.getPricingCapabilities([acceptance.crop]);
      BidAcceptanceService.sendBidAcceptanceEmail(
        context,
        supplyProfile,
        productionLocation,
        acceptance,
        demandOrder,
        pricing,
        bid
      );

      return new BidAcceptanceDTO(acceptance, pricing);
    } else {
      log.error(`Failed to create acceptance`);
    }
  }

  // TODO: Remove once we have solidified permissioning.
  public static async verifyOwnerOrFullGMA(context: Context, supplyProfile: MarketplaceSupplyProfileModel) {
    if (!context.user) {
      throw new Error(`user not present in context, cannot determine if owner.`);
    }

    const connector = AccountUserManagementConnector.getInstance({
      authToken: context.token,
      customEndpoint: Config.getString('IA_MARKETPLACE_CUSTOM_ACCOUNT_USER_MANAGEMENT_ENDPOINT'),
      environment: Config.getString('IA_ENV')
    });

    const account = await connector.getAccountOwnerByAccountId(context, supplyProfile.accountUid);
    if (!account) {
      throw new Error(`cannot find account: ${supplyProfile.accountUid}`);
    }

    if (await this.verifyUserHasRoleOnAccount(context, account.id, RoleNames.ACCOUNT_GRAIN_MARKETING_ADVISOR_FULL)) {
      log.info(`verified ${context.user.id} is GMA full access user of account ${supplyProfile.accountUid}`);
      return;
    }

    if (await this.verifyUserHasRoleOnAccount(context, account.id, RoleNames.ACCOUNT_GRAIN_MARKETING_ADVISOR_PENDING)) {
      throw new Error(
        `GMA user ${context.user.id} is not yet approved to transact on behalf of ${supplyProfile.accountUid}`
      );
    }

    log.info(JSON.stringify(context.user));
    if (account.owner) {
      if (context.user.id !== account.owner.id) {
        throw new Error(`user ${context.user.id} is not the owner of account ${supplyProfile.accountUid}`);
      }
    } else {
      const accountUserEdges = await connector.getAccountUserEdgesByAccountId(context, supplyProfile.accountUid);
      if (accountUserEdges.length === 1 && accountUserEdges[0].node.id !== context.user.id) {
        throw new Error(`user ${context.user.id} is not the owner of account ${supplyProfile.accountUid}`);
      } else if (accountUserEdges.length === 0 && context.user.primaryAccountId !== account.id) {
        throw new Error(`user ${context.user.id} is not related to account ${supplyProfile.accountUid}`);
      } else if (accountUserEdges.length > 1) {
        throw new Error(`unable to determine owner of account ${supplyProfile.accountUid}`);
      }
    }

    log.info(`verified ${context.user.id} is owner of account ${supplyProfile.accountUid}`);
  }

  public static async sendBidAcceptanceEmail(
    context: Context,
    { accountUid, uid: growerSupplyId }: MarketplaceSupplyProfileModel,
    growerLocation: ProductionLocationModel,
    {
      uid: acceptanceId,
      basisMonthCode,
      basisYear,
      crop,
      deliveryAddressId,
      createdAt,
      initiatedBy,
      cropQuantity,
      cropQualityConstraint,
      referenceId,
      sellerBasisValue,
      sellerPrice,
      shippingDistance,
      shippingProvider
    }: MarketplaceAcceptanceModel,
    {
      contactEmail,
      contactPhoneNumber,
      deliveryAtEnd,
      deliveryAtStart,
      indigoFee,
      price: buyerPriceValue,
      basisMonthCode: buyerBasisMonth,
      basisYear: buyerBasisYear,
      basisValue: buyerBasisValue,
      uid,
      demandProfileUid,
      type
    }: DemandOrderModel,
    pricing: PricingCapabilitiesDTO,
    { metadata }: BidAPIBid
  ) {
    const growerAccount = await UserService.getAccount(context, accountUid, `{ id name owner { id email } }`);
    if (!growerAccount) {
      throw new TypeNotFoundError(`Unable to find grower account ${accountUid} to send acceptance email`);
    }
    const sellerAccount = await UserService.getUsersGlobalAccount(
      context,
      context.user.id,
      `{ id name owner { id email } }`
    );
    if (!sellerAccount) {
      throw new TypeNotFoundError(
        `Unable to find global seller account for user ${context.user.id} to send acceptance email`
      );
    }

    const [deliveryAddress] = await AddressDAL.findAll({ id: deliveryAddressId });
    const demandProfile = await MarketplaceDemandProfileService.findById(demandProfileUid);
    const priceBasis = createBasisPrice({ basisMonthCode, basisYear }, sellerBasisValue, pricing);
    const buyerPriceBasis = createBasisPrice(
      { basisMonthCode: buyerBasisMonth, basisYear: buyerBasisYear },
      buyerBasisValue,
      pricing
    );
    const bushelsSold = numberWithCommas(cropQuantity);
    const contractType = priceBasis ? 'Basis' : 'Cash';
    const cropDisplay = cropLabel(crop);
    const dateSold = dateTimeString(createdAt);
    const deliveryWindow = dateRange(deliveryAtStart.toISOString(), deliveryAtEnd.toISOString());
    const lockInPriceDeadline = contractType === 'Basis' ? basisLockInPriceDeadline(priceBasis) : undefined;
    const quality = cropQuality(cropQualityConstraint);
    const growerPrice = priceBasis
      ? getPriceValue(priceBasis.value, priceBasis.monthCode, priceBasis.year)
      : getPriceValue(sellerPrice);
    const buyerPrice = buyerPriceBasis
      ? getPriceValue(buyerPriceBasis.value, buyerPriceBasis.monthCode, buyerPriceBasis.year)
      : getPriceValue(buyerPriceValue);
    const distance = getDistanceDisplay(deliveryAddress, shippingDistance);
    const isIndigoCertified = _.get(cropQualityConstraint, 'indigoCertified', false);
    const bidId = referenceId;
    const buyerName = demandProfile
      ? await UserService.getAccountName(context, demandProfile.facilityAccountId, demandProfile.name)
      : '';
    const demandOrderUid = uid;
    const growerId = growerAccount.id;
    const sellerAccountName = sellerAccount.name;
    const indigoFeeDisplay = getCommaSeparatedDollars(indigoFee, undefined, false);
    const dtnElevatorId = type === BidType.INDIGO_ALTERNATIVE_BID ? metadata.sourceElevatorId : undefined;

    const emailService = new EmailService();
    const sellerName = `${context.user.firstName} ${context.user.lastName}`;
    const growerName = `${context.user.firstName} ${context.user.lastName}`;
    const isSellerGma = await GMAService.isContextUserGma(context);
    const connectedGmaUser = await GMAService.getAssociatedGMAUser(sellerAccount.id, context);
    const data: SendBidAcceptanceForGrower = {
      bushelsSold,
      buyerAddress: `${deliveryAddress.city}, ${deliveryAddress.state}`,
      connectedGmaEmail: connectedGmaUser ? connectedGmaUser.email : null,
      contractType,
      crop: cropDisplay,
      dateSold,
      deliveryWindow,
      distance,
      growerAccountName: growerAccount.name,
      growerAddress: `${growerLocation.city}, ${growerLocation.state}`,
      growerEmail: growerAccount.owner.email,
      growerName,
      isSellerGma,
      lockInPriceDeadline,
      price: growerPrice,
      quality,
      saleType: SaleType.SELLER_INITIATED,
      sellerAccountName,
      sellerActingCapacity: UserService.getSellerActingCapacity(context.user.id, growerAccount.owner.id),
      sellerEmail: context.user.email,
      sellerName,
      shippingProvider
    };
    const sentTo: string[] = await emailService.sendEmailsToAccountEdge(
      data,
      growerAccount.id,
      context,
      EmailService.prototype.sendBidAcceptanceToGrower
    );
    await GMAService.handleGmaGrowerEmail(
      data,
      growerAccount.id,
      emailService,
      sentTo,
      EmailService.prototype.sendBidAcceptanceToGrower
    );
    log.info(`Sending acceptance email to support team for match on ${demandOrderUid}`);
    try {
      await emailService.sendBidAcceptanceEmail({
        bidId,
        bidRefId: referenceId,
        bushelsSold,
        buyerAddress: `${deliveryAddress.city}, ${deliveryAddress.state}`,
        buyerEmail: contactEmail,
        buyerName,
        buyerPhoneNumber: formatPhoneNumber(contactPhoneNumber),
        buyerPrice,
        contractTerms: quality,
        crop: cropDisplay,
        deliveryAddress: GeoService.toAddressString(deliveryAddress),
        deliveryWindow,
        demandOrderUid,
        dtnElevatorId,
        growerAccountName: growerAccount.name,
        growerAddress: `${growerLocation.city}, ${growerLocation.state}`,
        growerEmail: growerAccount.owner.email,
        growerId,
        growerMatchId: acceptanceId,
        growerPrice,
        growerSupplyId,
        indigoFee: indigoFeeDisplay,
        isIndigoCertified,
        pickupAddress: GeoService.toAddressString(growerLocation),
        price: growerPrice,
        sellerAccountName,
        sellerEmail: context.user.email,
        sellerId: sellerAccount.id,
        sellerName,
        sellerPhoneNumber: formatPhoneNumber(context.user.mobile),
        shippingProvider
      });
    } catch (err) {
      const msg = `Failed to send acceptance email to support team for match on ${demandOrderUid} with error ${err}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }
  }

  public static async getOriginatingUser(context: Context, userId: string) {
    if (!userId) {
      throw Error("User ID was null/undefined, can't get the originating user!");
    }

    const queryString = `query user(
      $id: ID!
    ) {
      user (
        where: { id: $id }
      ) {
        id
        email
        firstName
        lastName
        permissions
      }
    }
    `;
    const result = await UserService.customQuery(context, queryString, { id: userId });
    return result.user;
  }

  static async findAllPaginated(
    where: MarketplaceAcceptanceWhereInput,
    orderBy: [MarketplaceAcceptanceOrderByInput],
    pagination: LimitOffsetPaginationInput
  ) {
    const options: IFindOptions<MarketplaceAcceptanceModel> = {
      include: [],
      order: BidAcceptanceService.getMarketplaceAcceptancesSortExpression(orderBy),
      ...pagination,
      where: BidAcceptanceService.getMarketplaceAcceptanceWhereOptions(where)
    };

    const needToIncludeDemandOrder = !!where.demandOrderId || orderBy.find(item => item.startsWith('deliveryAtStart'));

    if (needToIncludeDemandOrder) {
      const includeExpression: IIncludeOptions = where.demandOrderId
        ? { model: DemandOrderModel, where: { uid: where.demandOrderId } }
        : { model: DemandOrderModel };

      options.include.push(includeExpression);
    }

    if (where.marketplaceOrderId) {
      const includeExpression: IIncludeOptions = {
        model: MarketplaceOrderModel,
        where: { uid: where.marketplaceOrderId }
      };

      options.include.push(includeExpression);
    }

    if (where.marketplaceOrderReferenceId) {
      const includeExpression: IIncludeOptions = {
        model: MarketplaceOrderModel,
        where: { referenceId: where.marketplaceOrderReferenceId }
      };

      options.include.push(includeExpression);
    }

    const acceptanceListInfo = await DAL.findAndCountAll(MarketplaceAcceptanceModel, options);
    const pricing = await PricingService.getPricingCapabilitiesMap(acceptanceListInfo.results.map(a => a.crop));
    return {
      data: acceptanceListInfo.results.map(model => new BidAcceptanceDTO(model, pricing[model.crop])),
      hasNextPage: acceptanceListInfo.hasNextPage,
      hasPreviousPage: acceptanceListInfo.hasPreviousPage,
      total: acceptanceListInfo.total
    };
  }

  public static validateAssignBasisToHedgeInput(
    demandOrder: DemandOrderModel,
    marketplaceOrder: MarketplaceOrderModel,
    supplyProfile: MarketplaceSupplyProfileModel,
    input: AssignBasisToHedgeOrderInput
  ) {
    if (!demandOrder) {
      throw new TypeNotFoundError(`DemandOrder ${input.demandOrderId} not found`);
    }
    if (!marketplaceOrder) {
      throw new TypeNotFoundError(`MarketplaceOrder ${input.marketplaceOrderId} not found`);
    }
    if (!supplyProfile) {
      throw new TypeNotFoundError(`Supply profile ${input.sellerId} not found`);
    }
    if (marketplaceOrder.product !== MarketOrderProduct.HEDGE) {
      throw new UserInputError(
        `Cannot assign basis to hedge for a marketplaceOrder ${marketplaceOrder.id} that has product: ${
          marketplaceOrder.product
        }`
      );
    }
    if (
      [MarketOrderStatus.CLOSED, MarketOrderStatus.PENDING, MarketOrderStatus.WORKING].includes(marketplaceOrder.status)
    ) {
      throw new UserInputError(
        `Cannot assign basis to hedge for a marketplaceOrder ${marketplaceOrder.id} that has status: ${
          marketplaceOrder.status
        }`
      );
    }
    if (marketplaceOrder.contractedQuantity >= marketplaceOrder.cropQuantity) {
      throw new UserInputError(
        `Cannot assign basis to hedge for a marketplaceOrder ${marketplaceOrder.id} that is already fully contracted`
      );
    }
    if (input.quantity > marketplaceOrder.availableToContractQuantity) {
      throw new UserInputError(
        `Cannot assign basis to hedge for a marketplaceOrder ${marketplaceOrder.id} the quantity:${
          input.quantity
        } is greater that what is available to contract: ${marketplaceOrder.availableToContractQuantity}.`
      );
    }
    if (
      marketplaceOrder.basisYear !== demandOrder.basisYear ||
      marketplaceOrder.basisMonthCode !== demandOrder.basisMonthCode
    ) {
      throw new UserInputError(
        `Cannot assign basis to hedge when the future reference month / year of the bid: ${
          demandOrder.basisMonthCode
        }/${demandOrder.basisYear} do not match the future reference month / year of the hedge: ${
          marketplaceOrder.basisMonthCode
        }/${marketplaceOrder.basisYear}.`
      );
    }
  }

  public static validateAssignBasisToHedgePricing(
    demandOrder: DemandOrderModel,
    marketplaceOrder: MarketplaceOrderModel,
    input: AssignBasisToHedgeOrderInput,
    bid: BidAPIBid
  ) {
    if (input.price.cashInput) {
      throw new NestedError(
        `Cannot assign basis for demandOrder: ${demandOrder.id} to a hedge: ${marketplaceOrder.id} with a cash price.`
      );
    }

    if (!demandOrder.isBasisOffer()) {
      throw new NestedError(
        `Cannot assign basis for cash price demandOrder: ${demandOrder.id} to a hedge: ${marketplaceOrder.id}.`
      );
    }

    if (bid.pricingDetails.__typename === 'CashPrice') {
      throw new NestedError(`Cannot assign basis for cash price bid to a hedge: ${marketplaceOrder.id}.`);
    } else {
      const basisPrice = bid.pricingDetails;
      const inputPrice = input.price.basisInput;
      if (
        basisPrice.month !== inputPrice.month ||
        basisPrice.year !== inputPrice.year ||
        // Add the indigo fee back in since the bid won't have it removed, but the grower saw a lower price
        _.round(basisPrice.value, PRICE_DECIMAL_PLACES) !==
          _.round(inputPrice.value + demandOrder.indigoFee, PRICE_DECIMAL_PLACES)
      ) {
        throw new NestedError(
          `Basis bid price ${JSON.stringify(basisPrice)} does not match given acceptance price ${JSON.stringify(
            inputPrice
          )}`
        );
      }
    }

    if (!marketplaceOrder.futuresReferencePrice) {
      throw new NestedError(`Cannot assign basis to order ${marketplaceOrder.id} without futures reference price set`);
    }
  }

  private static async verifyUserHasRoleOnAccount(context: Context, accountId: string, roleName: RoleNames) {
    const userQuery = `query user($where: UserWhereUniqueInput) {
      user(where: $where) {
        accounts(where: {roleNames: ["${roleName}"], id_in: ["${accountId}"]}) {
          edges {
            node {
              id
            }
          }
        }
      }
    }`;

    const userWithAccounts = (await UserService.customQuery(context, userQuery, { where: { id: context.user.id } }))
      .user as User;
    if (
      userWithAccounts &&
      userWithAccounts.accounts &&
      userWithAccounts.accounts.edges &&
      userWithAccounts.accounts.edges.length
    ) {
      return true;
    }
    return false;
  }
  private static async createAcceptanceWithAddresses(
    context: Context,
    input: AcceptBidInput,
    bid: BidAPIBid,
    demandOrder: DemandOrderModel,
    productionLocation: ProductionLocationModel,
    transaction: Transaction,
    marketplaceOrderId?: number,
    basisLockedInPrice?: number
  ) {
    // create pickup address
    let pickUpAddress = null;
    if (productionLocation.address) {
      pickUpAddress = productionLocation.address;
    } else {
      pickUpAddress = await AddressDAL.create(
        {
          city: productionLocation.city,
          country: productionLocation.country,
          county: productionLocation.county,
          postalCode: productionLocation.postalCode,
          state: productionLocation.state,
          street: productionLocation.street
        },
        { latitude: productionLocation.latitude, longitude: productionLocation.longitude },
        { transaction }
      );
    }

    // create delivery address if not an APB
    let deliveryAddress = null;
    if (demandOrder.type !== BidType.INDIGO_ALTERNATIVE_BID) {
      deliveryAddress = await AddressDAL.create(
        {
          city: demandOrder.deliveryCity,
          country: demandOrder.deliveryCountry,
          county: demandOrder.deliveryCounty,
          postalCode: demandOrder.deliveryPostalCode,
          state: demandOrder.deliveryState,
          street: demandOrder.deliveryStreet
        },
        { latitude: demandOrder.point.coordinates[1], longitude: demandOrder.point.coordinates[0] },
        { transaction }
      );
    }
    // create acceptance record
    return BidAcceptanceDAL.create(
      input,
      demandOrder,
      bid,
      MarketplaceAcceptanceInitiatedBy.SELLER,
      pickUpAddress.id,
      deliveryAddress ? deliveryAddress.id : null,
      context.user ? context.user.id : null,
      context.user ? `${context.user.firstName} ${context.user.lastName}` : null,
      marketplaceOrderId,
      basisLockedInPrice,
      {
        transaction
      }
    );
  }

  private static async getBid(
    demandOrder: DemandOrderModel,
    input: AcceptBidInput,
    supplyLocation: GeoPointInput,
    context: Context
  ): Promise<BidAPIBid> {
    const bidForTargetInput: Partial<BidForTargetInput> = {};
    if (demandOrder.type === BidType.INDIGO_ALTERNATIVE_BID) {
      // APBs do not have base prices, use futuresWindow
      bidForTargetInput.futuresWindow = {
        deliveryAtEnd: demandOrder.deliveryAtEnd,
        deliveryAtStart: demandOrder.deliveryAtStart
      };
    } else {
      // buyer bids
      const benchmarkPrice: Partial<BidAPIBenchmarkPriceInput> = {};
      // cash price bids
      if (demandOrder.price) {
        benchmarkPrice.cashPrice = {
          value: demandOrder.price
        };
      } else {
        // basis price bids
        benchmarkPrice.basisPrice = {
          month: demandOrder.basisMonthCode,
          value: demandOrder.basisValue,
          year: demandOrder.basisYear
        };
      }
      benchmarkPrice.deliveryAtStart = demandOrder.deliveryAtStart;
      benchmarkPrice.deliveryAtEnd = demandOrder.deliveryAtEnd;
      benchmarkPrice.location = {
        latitude: demandOrder.point.coordinates[1],
        longitude: demandOrder.point.coordinates[0]
      };
      bidForTargetInput.benchmarkPrice = benchmarkPrice as BidAPIBenchmarkPriceInput;
    }
    bidForTargetInput.crop = demandOrder.crop;
    bidForTargetInput.quantity = input.quantity;
    bidForTargetInput.shippingProvider = input.shippingProvider;
    bidForTargetInput.supplyLocation = supplyLocation;

    const bidDAL = new BidAPIDAL();
    const bid: BidAPIBid = await bidDAL.getBidForTarget(context, bidForTargetInput as BidForTargetInput);
    if (!bid) {
      throw new Error(`Could not get a bid from bid-api ${JSON.stringify(bidForTargetInput)}`);
    }
    return bid;
  }

  private static async findById(id: string) {
    return BidAcceptanceDAL.findById(id);
  }

  private static async sendOfferAcceptanceEmail(
    marketplaceOrder: MarketplaceOrderModel,
    acceptance: MarketplaceAcceptanceModel,
    supplyProfile: MarketplaceSupplyProfileModel,
    buyerProfile: MarketplaceDemandProfileModel,
    context: Context
  ) {
    const growerAccount = await UserService.getAccount(
      context,
      supplyProfile.accountUid,
      `{ id name owner { id email } }`
    );
    if (!growerAccount) {
      throw new TypeNotFoundError(
        `Unable to find account ${supplyProfile.accountUid} to send grower offer acceptance email.`
      );
    }

    // get the user who actually created the offer (may or may not be the same as the grower)
    const originatingUser = await BidAcceptanceService.getOriginatingUser(context, marketplaceOrder.createdBy);
    if (!originatingUser) {
      throw new TypeNotFoundError(
        `Unable to find seller user for ID ${marketplaceOrder.createdBy} to send grower offer acceptance email ` +
          `on order ID ${marketplaceOrder.uid}.`
      );
    }

    const connectedGmaUser = await GMAService.getAssociatedGMAUser(growerAccount.id, context);

    const genericEmailData: MarketplaceOrderAcceptanceEmailData = {
      acceptanceUid: acceptance.uid,
      address: {
        city: buyerProfile.city,
        country: buyerProfile.country,
        county: buyerProfile.county,
        postalCode: buyerProfile.postalCode,
        state: buyerProfile.state,
        street: buyerProfile.street
      },
      buyerName: await UserService.getAccountName(context, buyerProfile.facilityAccountUid, buyerProfile.name),
      connectedGmaEmail: connectedGmaUser ? connectedGmaUser.email : null,
      cropLabel: cropLabel(acceptance.crop),
      cropQuantity: numberWithCommas(acceptance.cropQuantity),
      cropQuantityUnit: toTitleCase(acceptance.cropUnit),
      deliveryMethod: toTitleCase(ShippingProvider[acceptance.shippingProvider]),
      expiresAt: dateTimeString(marketplaceOrder.expiresAt),
      growerAccountName: growerAccount.name,
      growerAddress: `${marketplaceOrder.address.city}, ${marketplaceOrder.address.state}`,
      growerEmail: growerAccount.owner.email,
      isSellerGma: GMAService.userHasGmaPermissions(originatingUser.permissions),
      price: marketplaceOrder.isBasisOffer()
        ? getPriceValue(marketplaceOrder.basisValue, acceptance.basisMonthCode, acceptance.basisYear)
        : getCommaSeparatedDollars(marketplaceOrder.cashPrice),
      pricingType: marketplaceOrder.isBasisOffer() ? 'Basis' : 'Cash',
      referenceId: acceptance.referenceId,
      saleType: SaleType.BUYER_INITIATED,

      sellerActingCapacity: UserService.getSellerActingCapacity(context.user.id, originatingUser.id),
      sellerEmail: originatingUser.email,
      sellerName: `${originatingUser.firstName} ${originatingUser.lastName}`
    };

    // Growers see expiration differently than buyers if the order expiration type is good til cancel
    const growerEmailData = {
      ...genericEmailData,
      expiresAt:
        marketplaceOrder.expirationType === MarketplaceOrderExpirationType.EXPIRATION_DATE
          ? dateTimeString(marketplaceOrder.expiresAt)
          : 'Good til Cancel'
    };

    const emailService = new EmailService();

    // Send emails to grower entities.
    log.info(
      `Sending grower offer acceptance confirmation to grower entities for
        accountUid ${supplyProfile.accountUid} for MarketplaceAcceptanceModel: ${JSON.stringify(acceptance)}`
    );

    const sentTo: string[] = await emailService.sendEmailsToAccountEdge<MarketplaceOrderAcceptanceEmailData>(
      growerEmailData,
      supplyProfile.accountUid,
      context,
      EmailService.prototype.sendMarketplaceOrderAcceptedToGrower
    );

    await GMAService.handleGmaGrowerEmail(
      genericEmailData,
      supplyProfile.accountUid,
      emailService,
      sentTo,
      EmailService.prototype.sendMarketplaceOrderAcceptedToGrower
    );

    // Send emails to buyer entities.
    log.info(
      `Sending grower offer acceptance confirmation to buyer entities for
        accountUid ${supplyProfile.accountUid} for MarketplaceAcceptanceModel: ${JSON.stringify(acceptance)}`
    );
    await emailService.sendEmailsToAccountEdge<MarketplaceOrderAcceptanceEmailData>(
      genericEmailData,
      buyerProfile.accountUid,
      context,
      EmailService.prototype.sendMarketplaceOrderAcceptedToBuyer
    );

    // Send emails to internal teams.
    try {
      log.info(
        `Sending grower offer acceptance confirmation to grower support team for offer acceptance uid  ${
          acceptance.uid
        }`
      );
      await emailService.sendMarketplaceOrderAcceptedToGrowerSupport(growerEmailData);

      log.info(
        `Sending grower offer acceptance confirmation to buyer support team for offer acceptance uid ${acceptance.uid}`
      );
      await emailService.sendMarketplaceOrderAcceptedToBuyerSupport(genericEmailData);
    } catch (err) {
      const msg = `Marketplace order acceptance was saved with acceptance uid: ${
        acceptance.uid
      } but a confirmation email failed to send to one of the internal teams with error message: ${err.message}`;
      log.error(msg);
      ErrorReporter.capture(msg);
    }
    return Promise.resolve();
  }

  private static getMarketplaceAcceptanceWhereOptions(where: MarketplaceAcceptanceWhereInput) {
    const { buyerId: buyerUid, id: uid, sellerId: sellerUid, crop, referenceId, initiatedBy } = where;
    const whereOptions: WhereOptions<MarketplaceAcceptanceModel> = {};

    if (crop) {
      whereOptions.crop = crop;
    }

    if (referenceId) {
      whereOptions.referenceId = referenceId;
    }

    if (initiatedBy) {
      whereOptions.initiatedBy = initiatedBy;
    }

    if (buyerUid) {
      whereOptions.buyerUid = buyerUid;
    }

    if (uid) {
      whereOptions.uid = uid;
    }

    if (sellerUid) {
      whereOptions.sellerUid = sellerUid;
    }

    return whereOptions;
  }

  private static getMarketplaceAcceptancesSortExpression(orderByInput: [MarketplaceAcceptanceOrderByInput]) {
    return orderByInput.map(input => {
      const [orderBy, sortDirection] = input.split('_');

      if (orderBy === 'deliveryAtStart') {
        return [{ model: DemandOrderModel, as: 'demandOrder' }, 'deliveryAtStart', sortDirection];
      } else {
        return [orderBy, sortDirection];
      }
    });
  }
}
