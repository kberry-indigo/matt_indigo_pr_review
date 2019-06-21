/*
  Add some testing utility functions to the marketplace orders that get built that
  help with converting the order to params the resolvers either expect as input or return as
  values.

  See https://www.bennadel.com/blog/3290-using-module-augmentation-to-safely-inject-runtime-methods-using-typescript-and-node-js.htm
  for an explanation of how we are doing the typescript wrangling
 */

import {
  AddressInput,
  CreateGrowerMarketplaceOrderInput,
  CreateHedgeOrderInput,
  CropPricingInput,
  FuturesReferencePriceInput
} from '@indigo-ag/schema';
import MarketplaceOrderModel from '../../src/modules/marketplaceOrder/marketplaceOrder.model';
import { isNil } from 'lodash';

declare module '../../src/modules/marketplaceOrder/marketplaceOrder.model' {
  // If we want to add INSTANCE METHODS to one of the application Classes, we have
  // to "declaration merge" an interface into the existing class.

  interface MarketplaceOrderModel {
    toGrowerOfferCreateInput(): CreateGrowerMarketplaceOrderInput;
    toCreateHedgeOrderInput(): CreateHedgeOrderInput;
  }
}

export function toGrowerOfferCreateInput(): CreateGrowerMarketplaceOrderInput {
  const address = this.address;
  let pickupAddress: AddressInput = null;
  if (address) {
    pickupAddress = {
      street: address.street,
      state: address.state,
      postalCode: address.postalCode,
      county: address.county,
      country: address.country,
      city: address.city
    };
  }

  let price: CropPricingInput = null;
  if (!isNil(this.cashPrice)) {
    price = { cashInput: { currencyCode: this.cashPriceCurrencyCode, value: this.cashPrice } };
  } else {
    price = { basisInput: { month: this.basisMonthCode, year: this.basisYear, value: this.basisValue } };
  }

  return {
    crop: this.crop,
    cropQuality: this.cropQuality,
    cropQuantity: this.cropQuantity,
    cropQuantityUnit: this.cropQuantityUnit,
    deliveryWindowEndAt: this.deliveryWindowEndAt.toISOString(),
    deliveryWindowStartAt: this.deliveryWindowStartAt.toISOString(),    expiresAt: this.expiresAt.toISOString(),
    expirationType: this.expirationType,
    maxRadiusMiles: this.maxRadiusMiles,
    notes: this.notes,
    pickupAddress,
    price,
    supplyProfileId: this.supplyProfile ? this.supplyProfile.uid : '',
    type: this.type,
    supportedShippingProviders: this.supportedShippingProviders,
    variety: this.variety
  };
}

export function toCreateHedgeOrderInput(): CreateHedgeOrderInput {
  const address = this.address;
  let pickupAddress: AddressInput = null;
  if (address) {
    pickupAddress = {
      street: address.street,
      state: address.state,
      postalCode: address.postalCode,
      county: address.county,
      country: address.country,
      city: address.city
    };
  }

  let price: FuturesReferencePriceInput = {
    month: this.basisMonthCode,
    value: this.futuresReferencePrice,
    year: this.basisYear,
    currencyCode: this.cashPriceCurrencyCode
  };

  return {
    crop: this.crop,
    cropQuality: this.cropQuality,
    cropQuantity: this.cropQuantity,
    cropQuantityUnit: this.cropQuantityUnit,
    deliveryWindowEndAt: this.deliveryWindowEndAt.toISOString(),
    deliveryWindowStartAt: this.deliveryWindowStartAt.toISOString(),
    expiresAt: this.expiresAt.toISOString(),
    expirationType: this.expirationType,
    maxRadiusMiles: this.maxRadiusMiles,
    notes: this.notes,
    pickupAddress,
    price,
    supplyProfileId: this.supplyProfile ? this.supplyProfile.uid : '',
    type: this.type,
    supportedShippingProviders: this.supportedShippingProviders,
    variety: this.variety
  };
}

MarketplaceOrderModel.prototype.toGrowerOfferCreateInput = toGrowerOfferCreateInput;
MarketplaceOrderModel.prototype.toCreateHedgeOrderInput = toCreateHedgeOrderInput;
