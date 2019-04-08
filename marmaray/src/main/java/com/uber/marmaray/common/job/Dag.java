package com.uber.marmaray.common.job;

import com.uber.marmaray.common.status.IStatus;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

public abstract class Dag {

    @NotEmpty @Getter @Setter
    private String jobName;

    @NotEmpty @Getter @Setter
    private String dataFeedName;

    @Getter @Setter
    private Map<String, String> jobManagerMetadata;

    public Dag(@NonNull final String jobName, @NonNull final String dataFeedName) {
        this.dataFeedName = dataFeedName;
        this.jobName = jobName;
    }

    public abstract IStatus execute();

}
