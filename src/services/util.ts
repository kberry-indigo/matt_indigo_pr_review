import { BidType, CashOrBasisPrice, Crop, CurrencyImpl, PricingCapabilities } from '@indigo-ag/schema';
import { NestedError } from '@indigo-ag/server';
import MarketplaceAcceptanceModel from '../modules/acceptances/acceptance.model';
import { MarketplaceBidDTO } from '../modules/bids';
import { MarketplaceCounterOfferModel } from '../modules/marketplaceNegotiation';
import { MarketplaceOrderModel } from '../modules/marketplaceOrder';
import { BasisModel, createBasisPrice } from '../modules/pricing';

const PRICE_MAXIMUM_BY_CROP: Map<Crop, number> = new Map<Crop, number>();
PRICE_MAXIMUM_BY_CROP.set(Crop.CORN_TWOYELLOW, 0.2);
PRICE_MAXIMUM_BY_CROP.set(Crop.CORN_TWOYELLOW_NONGM, 0.2);
PRICE_MAXIMUM_BY_CROP.set(Crop.SOYBEANS, 0.2);
PRICE_MAXIMUM_BY_CROP.set(Crop.SOYBEANS_NONGM, 0.2);

const GLOBAL_PRICE_MAXIMUM = 0.2;
const GLOBAL_PRICE_MINIMUM = -2.0;

export function isWithinPriceConstraints(bid: MarketplaceBidDTO): boolean {
  if (bid.type !== BidType.INDIGO_ALTERNATIVE_BID || _.isNil(bid.basisIndigoShips)) {
    return true;
  }

  const max = PRICE_MAXIMUM_BY_CROP.has(bid.crop) ? PRICE_MAXIMUM_BY_CROP.get(bid.crop) : GLOBAL_PRICE_MAXIMUM;
  const min = GLOBAL_PRICE_MINIMUM;

  return bid.basisIndigoShips.value <= max && bid.basisIndigoShips.value > min;
}
export function getPrice(
  model: MarketplaceAcceptanceModel | MarketplaceOrderModel | MarketplaceCounterOfferModel,
  cashValue: number,
  basisValue: number,
  basisLockedInPrice: number,
  pricing: PricingCapabilities
): CashOrBasisPrice {
  if (!_.isNil(cashValue)) {
    return new CurrencyImpl({ code: 'USD', value: cashValue });
  } else {
    const basisModel: BasisModel = {
      basisLockedInPrice,
      basisMonthCode: model.basisMonthCode,
      basisYear: model.basisYear
    };

    return createBasisPrice(basisModel, basisValue, pricing);
  }
}
