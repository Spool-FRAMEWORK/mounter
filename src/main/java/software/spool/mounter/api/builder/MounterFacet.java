package software.spool.mounter.api.builder;

abstract class MounterFacet<B> {
    protected final B parent;
    protected MounterFacet(B parent) { this.parent = parent; }
    public B and() { return parent; }
}
