package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuring Object for one State in the {@link ChainedEvaluationFunction}.
 * Contains all necessary information as DTO.
 *
 * @author julian
 * Created by julian on 05.09.18
 */
public class StateConfig implements Serializable {

    private final String stateAlias;
    private final EvaluationFunctionStateFactory factory;
    private final long stateTimeout;

    public StateConfig(String stateAlias, EvaluationFunctionStateFactory factory, long stateTimeout) {
        this.stateAlias = stateAlias;
        this.factory = factory;
        this.stateTimeout = stateTimeout;
    }

    /**
     * Create a new Instance of the Evaluation Function for this state.
     *
     * @return New Instance
     */
    public EvaluationFunction create() {
        return getFactory().create();
    }

    public String getStateAlias() {
        return stateAlias;
    }

    public EvaluationFunctionStateFactory getFactory() {
        return factory;
    }

    public long getStateTimeout() {
        return stateTimeout;
    }

    @Override
    public String toString() {
        return "StateConfig{" +
                "stateAlias='" + stateAlias + '\'' +
                ", factory=" + factory +
                ", stateTimeout=" + stateTimeout +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateConfig that = (StateConfig) o;
        return getStateTimeout() == that.getStateTimeout() &&
                Objects.equals(getStateAlias(), that.getStateAlias()) &&
                Objects.equals(getFactory(), that.getFactory());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStateAlias(), getFactory(), getStateTimeout());
    }
}
