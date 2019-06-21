import { IdUtility } from '@indigo-ag/common';
import {
  Crop,
  CropQualityConstraint,
  CropQuantityUnit,
  FuturesMonthCode,
  MarketOrderProduct,
  MarketOrderStatus,
  MarketOrderType,
  MarketplaceOrderExpirationType,
  ShippingProvider
} from '@indigo-ag/schema';
import {
  AllowNull,
  BeforeCreate,
  BelongsTo,
  Column,
  CreatedAt,
  DataType,
  Default,
  ForeignKey,
  HasOne,
  Model,
  Table,
  UpdatedAt
} from 'sequelize-typescript';
import { isBasisModel } from '../../services';
import AcceptanceModel from '../acceptances/acceptance.model';
import AddressModel from '../addresses/address.model';
import { DemandOrderModel } from '../orders';
import { MarketplaceSupplyProfileModel } from '../supply';

@Table({
  tableName: 'MarketplaceOrder',
  timestamps: true
})
export class MarketplaceOrderModel extends Model<MarketplaceOrderModel> {
  public static OPEN_ORDER_STATUSES = [MarketOrderStatus.OPEN, MarketOrderStatus.PENDING, MarketOrderStatus.WORKING];
  public static ACCEPTED_ORDER_STATUSES = [MarketOrderStatus.ACCEPTED, MarketOrderStatus.FILLED];
  public static CLOSED_ORDER_STATUSES = [MarketOrderStatus.CLOSED];

  @BeforeCreate
  static setReferenceId(instance: MarketplaceOrderModel) {
    instance.referenceId = `MO${IdUtility.generateHumanReadableId()}`;
  }

  @Default(() => IdUtility.generateUid())
  @Column
  uid: string;

  @Column
  referenceId: string;

  @AllowNull(false)
  @ForeignKey(() => MarketplaceSupplyProfileModel)
  @Column
  supplyProfileId: number;

  @AllowNull(false)
  @Column(DataType.STRING)
  product: MarketOrderProduct;

  @AllowNull(false)
  @Column(DataType.STRING)
  type: MarketOrderType;

  @BelongsTo(() => MarketplaceSupplyProfileModel, { foreignKey: 'supplyProfileId', targetKey: 'id' })
  supplyProfile: MarketplaceSupplyProfileModel;

  @HasOne(() => AcceptanceModel)
  acceptance: AcceptanceModel;

  @AllowNull(false)
  @Column(DataType.STRING)
  status: MarketOrderStatus;

  @Column(DataType.STRING)
  crop: Crop;

  @Column(DataType.STRING)
  variety: string;

  @AllowNull(false)
  @Column(DataType.DOUBLE)
  cropQuantity: number;

  @Column(DataType.DOUBLE)
  filledQuantity: number;

  @Column(DataType.DOUBLE)
  contractedQuantity: number;

  @AllowNull(false)
  @Column(DataType.STRING)
  cropQuantityUnit: CropQuantityUnit;

  @Column(DataType.JSONB)
  cropQuality: CropQualityConstraint;

  @AllowNull(false)
  @Column(DataType.JSONB)
  supportedShippingProviders: ShippingProvider[];

  @AllowNull(false)
  @Column(DataType.DATE)
  deliveryWindowStartAt: Date;

  @AllowNull(false)
  @Column(DataType.DATE)
  deliveryWindowEndAt: Date;

  @AllowNull(false)
  @Column(DataType.DATE)
  expiresAt: Date;

  @AllowNull(false)
  @Column(DataType.STRING)
  expirationType: MarketplaceOrderExpirationType;

  @AllowNull(false)
  @ForeignKey(() => AddressModel)
  @Column
  addressId: number;

  @BelongsTo(() => AddressModel, { foreignKey: 'addressId', targetKey: 'id' })
  address: AddressModel;

  @Column
  maxRadiusMiles: number;

  @Column
  cashPrice: number;

  @Column
  cashPriceCurrencyCode: string;

  @Column
  basisValue: number;

  @Column
  basisYear: number;

  @Column(DataType.STRING)
  basisMonthCode: FuturesMonthCode;

  @Column
  createdBy: string;

  @Column
  closedBy: string;

  @Column(DataType.DATE)
  closedAt: Date;

  @CreatedAt
  @Column(DataType.DATE)
  createdAt: Date;

  @UpdatedAt
  @Column(DataType.DATE)
  updatedAt: Date;

  @Column(DataType.TEXT)
  notes: string;

  // Denormalized value taken from Account table in ceres. If present, grower has opted to share.
  @Column(DataType.TEXT)
  accountName: string;

  @AllowNull(true)
  @ForeignKey(() => DemandOrderModel)
  @Column
  originatingDemandOrderId: string;

  @BelongsTo(() => DemandOrderModel, { foreignKey: 'originatingDemandOrderId', targetKey: 'id' })
  originatingDemandOrder: DemandOrderModel;

  @Column(DataType.DOUBLE)
  futuresReferencePrice: number;

  get availableToContractQuantity(): number {
    return this.filledQuantity - this.contractedQuantity;
  }

  isBasisOffer() {
    return isBasisModel({ basisValue: this.basisValue, cashValue: this.cashPrice });
  }

  isExpired(today: Date = new Date()) {
    return this.expiresAt < today;
  }

  isOpen() {
    return MarketplaceOrderModel.OPEN_ORDER_STATUSES.includes(this.status);
  }

  isAccepted() {
    return MarketplaceOrderModel.ACCEPTED_ORDER_STATUSES.includes(this.status);
  }

  isClosed() {
    return MarketplaceOrderModel.CLOSED_ORDER_STATUSES.includes(this.status);
  }
}

export default MarketplaceOrderModel;
