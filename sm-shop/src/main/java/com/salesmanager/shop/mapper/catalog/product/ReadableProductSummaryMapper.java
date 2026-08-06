package com.salesmanager.shop.mapper.catalog.product;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.salesmanager.core.business.exception.ServiceException;
import com.salesmanager.core.business.services.catalog.pricing.PricingService;
import com.salesmanager.core.model.catalog.product.Product;
import com.salesmanager.core.model.catalog.product.availability.ProductAvailability;
import com.salesmanager.core.model.catalog.product.description.ProductDescription;
import com.salesmanager.core.model.catalog.product.image.ProductImage;
import com.salesmanager.core.model.catalog.product.price.FinalPrice;
import com.salesmanager.core.model.catalog.product.price.ProductPrice;
import com.salesmanager.core.model.catalog.product.price.ProductPriceDescription;
import com.salesmanager.core.model.merchant.MerchantStore;
import com.salesmanager.core.model.reference.language.Language;
import com.salesmanager.shop.mapper.Mapper;
import com.salesmanager.shop.model.catalog.product.ReadableImage;
import com.salesmanager.shop.model.catalog.product.ReadableProduct;
import com.salesmanager.shop.model.catalog.product.ReadableProductPrice;
import com.salesmanager.shop.model.catalog.product.product.ProductSpecification;
import com.salesmanager.shop.model.references.DimensionUnitOfMeasure;
import com.salesmanager.shop.model.references.WeightUnitOfMeasure;
import com.salesmanager.shop.store.api.exception.ConversionRuntimeException;
import com.salesmanager.shop.utils.DateUtil;
import com.salesmanager.shop.utils.ImageFilePath;

@Component
public class ReadableProductSummaryMapper implements Mapper<Product, ReadableProduct> {

	@Autowired
	private PricingService pricingService;

	@Autowired
	@Qualifier("img")
	private ImageFilePath imageUtils;

	@Override
	public ReadableProduct convert(Product source, MerchantStore store, Language language) {
		return merge(source, new ReadableProduct(), store, language);
	}

	@Override
	public ReadableProduct merge(Product source, ReadableProduct destination, MerchantStore store, Language language) {
		Validate.notNull(source, "Product cannot be null");
		Validate.notNull(destination, "ReadableProduct cannot be null");

		destination.setId(source.getId());
		destination.setSku(source.getSku());
		destination.setRefSku(source.getRefSku());
		destination.setAvailable(source.isAvailable());
		destination.setProductShipeable(source.isProductShipeable());
		destination.setProductVirtual(source.getProductVirtual());
		destination.setPreOrder(source.isPreOrder());
		destination.setSortOrder(source.getSortOrder());

		if (source.getDateAvailable() != null) {
			destination.setDateAvailable(DateUtil.formatDate(source.getDateAvailable()));
		}

		if (source.getAuditSection() != null) {
			destination.setCreationDate(DateUtil.formatDate(source.getAuditSection().getDateCreated()));
		}

		if (source.getProductReviewAvg() != null) {
			double avg = source.getProductReviewAvg().doubleValue();
			destination.setRating(Math.round(avg * 2) / 2.0d);
		}

		if (source.getProductReviewCount() != null) {
			destination.setRatingCount(source.getProductReviewCount().intValue());
		}

		ProductDescription description = selectDescription(source, language);
		if (description != null) {
			destination.setDescription(populateDescription(description));
		}

		destination.setProductSpecifications(specifications(source, store));
		applyImages(source, destination, store);

		ProductAvailability availability = selectAvailability(source.getAvailabilities());
		if (availability != null) {
			applyAvailability(destination, availability);
			applyPrice(destination, availability, store, language);
		}

		return destination;
	}

	private ProductDescription selectDescription(Product source, Language language) {
		if (CollectionUtils.isEmpty(source.getDescriptions())) {
			return null;
		}
		if (language == null) {
			return source.getDescriptions().iterator().next();
		}
		return source.getDescriptions().stream()
				.filter(desc -> desc.getLanguage() != null && desc.getLanguage().getId().intValue() == language.getId().intValue())
				.findFirst()
				.orElse(source.getDescriptions().iterator().next());
	}

	private com.salesmanager.shop.model.catalog.product.ProductDescription populateDescription(ProductDescription description) {
		com.salesmanager.shop.model.catalog.product.ProductDescription target =
				new com.salesmanager.shop.model.catalog.product.ProductDescription();
		target.setDescription(description.getDescription());
		target.setFriendlyUrl(description.getSeUrl());
		target.setHighlights(description.getProductHighlight());
		target.setId(description.getId());
		target.setKeyWords(description.getMetatagKeywords());
		target.setLanguage(description.getLanguage().getCode());
		target.setMetaDescription(description.getMetatagDescription());
		target.setName(description.getName());
		target.setTitle(description.getMetatagTitle());
		return target;
	}

	private ProductSpecification specifications(Product source, MerchantStore store) {
		ProductSpecification specifications = new ProductSpecification();
		specifications.setHeight(source.getProductHeight());
		specifications.setLength(source.getProductLength());
		specifications.setWeight(source.getProductWeight());
		specifications.setWidth(source.getProductWidth());
		if (!StringUtils.isBlank(store.getSeizeunitcode())) {
			specifications.setDimensionUnitOfMeasure(DimensionUnitOfMeasure.valueOf(store.getSeizeunitcode().toLowerCase()));
		}
		if (!StringUtils.isBlank(store.getWeightunitcode())) {
			specifications.setWeightUnitOfMeasure(WeightUnitOfMeasure.valueOf(store.getWeightunitcode().toLowerCase()));
		}
		return specifications;
	}

	private void applyImages(Product source, ReadableProduct destination, MerchantStore store) {
		Set<ProductImage> images = source.getImages();
		if (CollectionUtils.isEmpty(images)) {
			return;
		}

		String contextPath = imageUtils.getContextPath();
		List<ReadableImage> readableImages = new ArrayList<ReadableImage>();

		for (ProductImage img : images) {
			ReadableImage readableImage = new ReadableImage();
			readableImage.setImageName(img.getProductImage());
			readableImage.setDefaultImage(img.isDefaultImage());
			readableImage.setOrder(img.getSortOrder() != null ? img.getSortOrder().intValue() : 0);
			readableImage.setId(img.getId());
			readableImage.setImageType(img.getImageType());
			readableImage.setExternalUrl(img.getProductImageUrl());

			if (img.getImageType() == 1 && img.getProductImageUrl() != null) {
				readableImage.setImageUrl(img.getProductImageUrl());
				readableImage.setVideoUrl(img.getProductImageUrl());
			} else {
				readableImage.setImageUrl(contextPath + imageUtils.buildProductImageUtils(store, source.getSku(), img.getProductImage()));
			}

			if (readableImage.isDefaultImage()) {
				destination.setImage(readableImage);
			}
			readableImages.add(readableImage);
		}

		destination.setImages(readableImages.stream()
				.sorted(Comparator.comparingInt(ReadableImage::getOrder))
				.collect(Collectors.toList()));
	}

	private ProductAvailability selectAvailability(Set<ProductAvailability> availabilities) {
		if (CollectionUtils.isEmpty(availabilities)) {
			return null;
		}
		Optional<ProductAvailability> defaultAvailability = availabilities.stream()
				.filter(availability -> availability.getProductVariant() == null && StringUtils.isEmpty(availability.getRegionVariant()))
				.findFirst();
		return defaultAvailability.orElse(availabilities.iterator().next());
	}

	private void applyAvailability(ReadableProduct destination, ProductAvailability availability) {
		int quantity = availability.getProductQuantity() == null ? 1 : availability.getProductQuantity();
		destination.setQuantity(quantity);
		destination.setQuantityOrderMaximum(
				availability.getProductQuantityOrderMax() == null ? 1 : availability.getProductQuantityOrderMax());
		destination.setQuantityOrderMinimum(
				availability.getProductQuantityOrderMin() == null ? 1 : availability.getProductQuantityOrderMin());
		if (quantity > 0 && destination.isAvailable()) {
			destination.setCanBePurchased(true);
		}
	}

	private void applyPrice(ReadableProduct destination, ProductAvailability availability, MerchantStore store, Language language) {
		try {
			FinalPrice price = pricingService.calculateProductPrice(availability);
			if (price == null) {
				return;
			}

			destination.setFinalPrice(pricingService.getDisplayAmount(price.getFinalPrice(), store));
			destination.setPrice(price.getFinalPrice());
			destination.setOriginalPrice(pricingService.getDisplayAmount(price.getOriginalPrice(), store));
			destination.setDiscounted(price.isDiscounted());

			Set<ProductPrice> prices = availability.getPrices();
			if (CollectionUtils.isEmpty(prices)) {
				return;
			}

			ReadableProductPrice readableProductPrice = new ReadableProductPrice();
			readableProductPrice.setDiscounted(destination.isDiscounted());
			readableProductPrice.setFinalPrice(destination.getFinalPrice());
			readableProductPrice.setOriginalPrice(destination.getOriginalPrice());
			destination.setProductPrice(readableProductPrice);

			Optional<ProductPrice> defaultPrice = prices.stream()
					.filter(productPrice -> productPrice.getCode().equals(ProductPrice.DEFAULT_PRICE_CODE))
					.findFirst();
			if (defaultPrice.isPresent() && language != null) {
				readableProductPrice.setId(defaultPrice.get().getId());
				defaultPrice.get().getDescriptions().stream()
						.filter(desc -> desc.getLanguage().getCode().equals(language.getCode()))
						.findFirst()
						.ifPresent(desc -> readableProductPrice.setDescription(priceDescription(desc, language)));
			}
		} catch (ServiceException e) {
			throw new ConversionRuntimeException("An error while converting product list price", e);
		}
	}

	private com.salesmanager.shop.model.catalog.product.ProductPriceDescription priceDescription(
			ProductPriceDescription source,
			Language language) {
		com.salesmanager.shop.model.catalog.product.ProductPriceDescription target =
				new com.salesmanager.shop.model.catalog.product.ProductPriceDescription();
		target.setLanguage(language.getCode());
		target.setId(source.getId());
		target.setPriceAppender(source.getPriceAppender());
		return target;
	}
}
