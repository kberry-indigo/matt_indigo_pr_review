import {
  Crop,
  CropQuantityUnit,
  FuturesMonthCode,
  MarketOrderProduct,
  MarketOrderStatus,
  MarketOrderType,
  MarketplaceOrderExpirationType,
  ShippingProvider
} from '@indigo-ag/schema';
import { factory } from 'factory-girl';
import * as moment from 'moment';
import MarketplaceOrderModel from '../../src/modules/marketplaceOrder/marketplaceOrder.model';

factory.define(
  'MarketplaceOrder',
  MarketplaceOrderModel,
  {
    supplyProfileId: factory.assoc('MarketplaceSupplyProfile', 'id'),
    product: MarketOrderProduct.OTC,
    type: MarketOrderType.SELL,
    status: MarketOrderStatus.OPEN,
    crop: Crop.SOYBEANS,
    variety: null,
    cropQuantity: 100,
    cropQuantityUnit: CropQuantityUnit.BUSHELS,
    filledQuantity: 0,
    contractedQuantity: 0,
    supportedShippingProviders: [ShippingProvider.BUYER],
    deliveryWindowStartAt: new Date(2019, 1, 1),
    deliveryWindowEndAt: new Date(2019, 1, 31),
    expiresAt: moment()
      .add(30, 'days')
      .toDate(),
    expirationType: MarketplaceOrderExpirationType.EXPIRATION_DATE,
    addressId: factory.assoc('Address', 'id'),
    maxRadiusMiles: 100,
    cashPrice: 0.75,
    cashPriceCurrencyCode: 'USD',
    createdBy: 'abc123',
    notes: null,
    cropQuality: null,
    futuresReferencePrice: null,
    // referenceId here will be overwritten by the beforeCreate hook in the DB
    referenceId: 'MO12345'
  },
  {
    afterBuild: async function(model: any) {
      /*
        Factory girl will create the associated models when you do a .build but it won't retrieve values.
        The following retrieves the associated objects after it is built.
       */
      model.supplyProfile = model.supplyProfile || (await model.getSupplyProfile());
      model.address = model.address || (await model.getAddress());
      return model;
    }
  }
);

factory.extend('MarketplaceOrder', 'MarketplaceOrderWithCashPrice', {
  basisValue: undefined,
  basisYear: undefined,
  basisMonthCode: undefined,
  cashPrice: 0.75,
  cashPriceCurrencyCode: 'USD'
});

factory.extend('MarketplaceOrder', 'MarketplaceOrderWithBasisPrice', {
  basisValue: 0.1,
  basisYear: 2019,
  basisMonthCode: FuturesMonthCode.M,
  cashPrice: undefined,
  cashPriceCurrencyCode: undefined
});

factory.extend('MarketplaceOrderWithBasisPrice', 'MarketplaceOrderWithFloatingBasis', {
  product: MarketOrderProduct.HEDGE,
  futuresReferencePrice: 5.43,
  cashPrice: undefined
});
