import { MarketplaceOrderImpl, PricingCapabilities } from '@indigo-ag/schema';
import { getPrice } from '../../services/util';
import MarketplaceOrderModel from './marketplaceOrder.model';

export class MarketplaceOrderDTO extends MarketplaceOrderImpl {
  public readonly supplyProfileId: number;
  public readonly addressId: number;
  public readonly marketplaceOrderDatabaseId: number;
  public readonly originatingDemandOrderId: string;

  constructor(obj: MarketplaceOrderModel, pricing: PricingCapabilities) {
    super({
      acceptance: null,
      accountName: obj.accountName,
      address: null,
      availableToContractQuantity: obj.availableToContractQuantity,
      closedAt: obj.closedAt ? obj.closedAt.toISOString() : null,
      closedBy: obj.closedBy,
      contractedQuantity: obj.contractedQuantity,
      createdAt: obj.createdAt.toISOString(),
      createdBy: obj.createdBy,
      crop: obj.crop,
      cropQuality: _.isEmpty(obj.cropQuality) ? undefined : obj.cropQuality,
      cropQuantity: obj.cropQuantity,
      cropQuantityUnit: obj.cropQuantityUnit,
      deliveryWindowEndAt: obj.deliveryWindowEndAt.toISOString(),
      deliveryWindowStartAt: obj.deliveryWindowStartAt.toISOString(),
      expirationType: obj.expirationType,
      expiresAt: obj.expiresAt.toISOString(),
      filledQuantity: obj.filledQuantity,
      id: obj.uid,
      maxRadiusMiles: obj.maxRadiusMiles,
      notes: obj.notes,
      orderFees: [],
      originatingDemandOrder: null,
      price: getPrice(obj, obj.cashPrice, obj.basisValue, obj.futuresReferencePrice, pricing),
      product: obj.product,
      referenceId: obj.referenceId,
      seller: null,
      status: obj.status,
      supportedShippingProviders: obj.supportedShippingProviders,
      type: obj.type,
      updatedAt: obj.updatedAt.toISOString(),
      variety: obj.variety
    });

    this.originatingDemandOrderId = obj.originatingDemandOrderId;
    this.addressId = obj.addressId;
    this.supplyProfileId = obj.supplyProfileId;
    this.marketplaceOrderDatabaseId = obj.id;
  }
}
