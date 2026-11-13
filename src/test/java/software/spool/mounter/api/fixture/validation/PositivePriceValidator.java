package software.spool.mounter.api.fixture.validation;

import software.spool.validator.api.Severity;
import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;

@Validate(value = Product.class, sourceId = "products", severity = Severity.WARNING)
public class PositivePriceValidator implements Validator<Product> {
    @Override
    public boolean validate(Product product) {
        return product.price() > 0;
    }
}
