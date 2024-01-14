package org.zhu45.treetracker.relational.planner.cost;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CardEstProviderStatistics
{
    @Getter
    private final String providerName;
    @Getter
    private final JoinTreeCostStatistics joinTreeCostStatistics;

    private CardEstProviderStatistics(Builder builder)
    {
        this.providerName = builder.providerName;
        this.joinTreeCostStatistics = builder.joinTreeCostStatistics;
    }

    public static Builder builder(Class<? extends CardEstProvider> cardEstProviderClazz)
    {
        return new Builder(cardEstProviderClazz);
    }

    public static class Builder
    {
        private final String providerName;
        private JoinTreeCostStatistics joinTreeCostStatistics;

        public Builder(Class<? extends CardEstProvider> cardEstProviderClazz)
        {
            this.providerName = cardEstProviderClazz.getCanonicalName();
        }

        public Builder joinTreeCostStatistics(JoinTreeCostStatistics joinTreeCostStatistics)
        {
            this.joinTreeCostStatistics = joinTreeCostStatistics;
            return this;
        }

        public CardEstProviderStatistics build()
        {
            return new CardEstProviderStatistics(this);
        }
    }
}
