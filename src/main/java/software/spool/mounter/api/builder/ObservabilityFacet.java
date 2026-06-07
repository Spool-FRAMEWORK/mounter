package software.spool.mounter.api.builder;

import software.spool.core.utils.routing.ErrorRouter;

public class ObservabilityFacet<B> extends MounterFacet<B> {
    ErrorRouter errorRouter;

    ObservabilityFacet(B parent) { super(parent); }

    public ObservabilityFacet<B> withErrorRouter(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        return this;
    }
}
