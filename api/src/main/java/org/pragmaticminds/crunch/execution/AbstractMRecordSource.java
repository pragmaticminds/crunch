package org.pragmaticminds.crunch.execution;

/**
 * Abstract implementation of {@link MRecordSource} which overrides init and close with empty methods.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public abstract class AbstractMRecordSource implements MRecordSource {

    private Kind kind;

    public AbstractMRecordSource(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind getKind() {
        return kind;
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
