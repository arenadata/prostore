package io.arenadata.dtm.query.execution.plugin.api.service.shared.adg;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service
public class AdgSharedServiceStub implements AdgSharedService {

    public static final String STUBBED_EXCEPTION = "Disabled plugin [ADG]";

    @Override
    public Future<Void> prepareStaging(AdgSharedPrepareStagingRequest request) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> transferData(AdgSharedTransferDataRequest request) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public AdgSharedProperties getSharedProperties() {
        throw new DtmException(STUBBED_EXCEPTION);
    }
}
