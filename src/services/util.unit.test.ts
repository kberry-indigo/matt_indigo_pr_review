import {
  BidType,
  CashOrBasisPrice,
  Crop,
  CropPricingType,
  FuturesMonthCode,
  PricingCapabilities,
  ShippingProvider
} from '@indigo-ag/schema';
import { MarketplaceAcceptanceModel } from '../modules/acceptances';
import { MarketplaceBidDTO } from '../modules/bids/bid.dto';
import { isWithinPriceConstraints, getPrice } from './util';

describe('isWithinPriceConstraints', () => {
  const alternativeOrder: any = {
    type: BidType.INDIGO_ALTERNATIVE_BID,
    supportedShippingProviders: [ShippingProvider.INDIGO, ShippingProvider.SELLER]
  };
  const partnerOrder: any = {
    type: BidType.PARTNER_BID,
    supportedShippingProviders: [ShippingProvider.INDIGO, ShippingProvider.SELLER]
  };
  it('Ignores non-basis, and indigo ships', () => {
    const bidResponse = { isBasis: false, priceIndigoShips: 0.01 } as any;
    const bid = new MarketplaceBidDTO(alternativeOrder, bidResponse, 'fooSellerId');
    expect(bid.priceIndigoShips.value).toBe(0.01);
    expect(isWithinPriceConstraints(bid)).toBe(true);
  });

  it('Ignores basis, no indigo ships', () => {
    const bidResponse = { isBasis: true, priceSellerShips: 0.01 } as any;
    const bid = new MarketplaceBidDTO(alternativeOrder, bidResponse, 'fooSellerId');
    expect(bid.basisSellerShips.value).toBe(0.01);
    expect(isWithinPriceConstraints(bid)).toBe(true);
  });

  it('Ignores non-APB', () => {
    const bidResponse = { isBasis: true, priceIndigoShips: 0.01 } as any;
    const bid = new MarketplaceBidDTO(partnerOrder, bidResponse, 'fooSellerId');
    expect(bid.basisIndigoShips.value).toBe(0.01);
    expect(isWithinPriceConstraints(bid)).toBe(true);
  });

  it('Applies crop specific constraint', () => {
    const crops = [Crop.CORN_TWOYELLOW, Crop.CORN_TWOYELLOW_NONGM, Crop.SOYBEANS, Crop.SOYBEANS_NONGM];
    crops.forEach(crop => {
      const bidResponsePass = { isBasis: true, priceIndigoShips: 0.01 } as any;
      const bidResponseAboveMax = { isBasis: true, priceIndigoShips: 0.3 } as any;
      const bidResponseBelowMin = { isBasis: true, priceIndigoShips: -3 } as any;
      const order = _.merge(alternativeOrder, { crop });
      const bidPass = new MarketplaceBidDTO(order, bidResponsePass, 'fooSellerId');
      const bidFail = new MarketplaceBidDTO(order, bidResponseAboveMax, 'fooSellerId');
      const bidBelowMinFail = new MarketplaceBidDTO(order, bidResponseBelowMin, 'fooSellerId');
      expect(isWithinPriceConstraints(bidPass)).toBe(true);
      expect(isWithinPriceConstraints(bidFail)).toBe(false);
      expect(isWithinPriceConstraints(bidBelowMinFail)).toBe(false);
    });
  });

  it('Applies global constraint', () => {
    const bidResponsePass = { isBasis: true, priceIndigoShips: 0.01 } as any;
    const bidResponseAboveMax = { isBasis: true, priceIndigoShips: 0.3 } as any;
    const bidResponseBelowMin = { isBasis: true, priceIndigoShips: -3 } as any;
    const order = _.merge(alternativeOrder, { crop: Crop.RYE });
    const bidPass = new MarketplaceBidDTO(order, bidResponsePass, 'fooSellerId');
    const bidAboveMaxFail = new MarketplaceBidDTO(order, bidResponseAboveMax, 'fooSellerId');
    const bidBelowMinFail = new MarketplaceBidDTO(order, bidResponseBelowMin, 'fooSellerId');
    expect(isWithinPriceConstraints(bidPass)).toBe(true);
    expect(isWithinPriceConstraints(bidAboveMaxFail)).toBe(false);
    expect(isWithinPriceConstraints(bidBelowMinFail)).toBe(false);
  });
});

describe('getPrice (cash input)', () => {
  let cashOrBasisPrice: CashOrBasisPrice;
  let marketplaceModel: MarketplaceAcceptanceModel;
  let pricingCaps: PricingCapabilities;

  afterEach(() => {
    marketplaceModel = undefined;
    pricingCaps = undefined;
    cashOrBasisPrice = undefined;
  });

  it('should return a cash price object when cash price is defined', () => {
    marketplaceModel = {
      basisMonthCode: FuturesMonthCode.M,
      basisYear: 2019
    } as any;
    pricingCaps = {
      crop: Crop.ALFALFA,
      supportedPricingTypes: [CropPricingType.CASH],
      pricingCalendar: []
    };
    cashOrBasisPrice = getPrice(marketplaceModel, 3.45, 0.25, 0.6, pricingCaps);
    expect(cashOrBasisPrice.value).toBe(3.45);
  });
});

describe('getPrice (basis input)', () => {
  let cashOrBasisPrice: CashOrBasisPrice;
  let marketplaceModel: MarketplaceAcceptanceModel;
  let pricingCaps: PricingCapabilities;
  let basisValue: number = 0.25;

  afterEach(() => {
    marketplaceModel = undefined;
    pricingCaps = undefined;
    cashOrBasisPrice = undefined;
  });

  describe('Basis price when cash price is undefined', () => {
    beforeEach(() => {
      marketplaceModel = {
        basisMonthCode: FuturesMonthCode.M,
        basisYear: 2019
      } as any;
      pricingCaps = {
        crop: Crop.ALFALFA,
        supportedPricingTypes: [CropPricingType.CASH],
        pricingCalendar: []
      };
      cashOrBasisPrice = getPrice(marketplaceModel, null, basisValue, 0.6, pricingCaps);
    });

    it('should return the correct basis value', () => {
      expect(cashOrBasisPrice.value).toBe(basisValue);
    });
  });

  describe('Basis price is null', () => {
    beforeEach(() => {
      marketplaceModel = {
        basisMonthCode: FuturesMonthCode.M,
        basisYear: 2019
      } as any;
      pricingCaps = {
        crop: Crop.ALFALFA,
        supportedPricingTypes: [CropPricingType.CASH],
        pricingCalendar: []
      };
      cashOrBasisPrice = getPrice(marketplaceModel, null, null, 0.6, pricingCaps);
    });

    it('should return an undefined CashOrBasisPrice object', () => {
      expect(cashOrBasisPrice).toBeUndefined();
    });
  });

  describe('Basis price is defined but month code is not', () => {
    beforeEach(() => {
      marketplaceModel = {
        basisMonthCode: null,
        basisYear: 2019
      } as any;
      pricingCaps = {
        crop: Crop.ALFALFA,
        supportedPricingTypes: [CropPricingType.CASH],
        pricingCalendar: []
      };
      cashOrBasisPrice = getPrice(marketplaceModel, null, basisValue, 0.6, pricingCaps);
    });

    it('should return an undefined CashOrBasisPrice object', () => {
      expect(cashOrBasisPrice).toBeUndefined();
    });
  });

  describe('Basis price is defined but year is not', () => {
    beforeEach(() => {
      marketplaceModel = {
        basisMonthCode: FuturesMonthCode.M,
        basisYear: null
      } as any;
      pricingCaps = {
        crop: Crop.ALFALFA,
        supportedPricingTypes: [CropPricingType.CASH],
        pricingCalendar: []
      };
      cashOrBasisPrice = getPrice(marketplaceModel, null, basisValue, 0.6, pricingCaps);
    });

    it('should return an undefined CashOrBasisPrice object', () => {
      expect(cashOrBasisPrice).toBeUndefined();
    });
  });
});
