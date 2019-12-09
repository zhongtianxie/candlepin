/**
 * Copyright (c) 2009 - 2012 Red Hat, Inc.
 *
 * This software is licensed to you under the GNU General Public License,
 * version 2 (GPLv2). There is NO WARRANTY for this software, express or
 * implied, including the implied warranties of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
 * along with this software; if not, see
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
 *
 * Red Hat trademarks are not licensed under GPLv2. No permission is
 * granted to use or replicate Red Hat trademarks that are incorporated
 * in this software or its documentation.
 */
package org.candlepin.async.tasks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.candlepin.async.JobConfig;
import org.candlepin.async.JobConfigValidationException;
import org.candlepin.async.JobExecutionContext;
import org.candlepin.async.JobExecutionException;
import org.candlepin.auth.Principal;
import org.candlepin.common.config.Configuration;;
import org.candlepin.config.CandlepinCommonTestConfig;
import org.candlepin.dto.ModelTranslator;
import org.candlepin.dto.StandardTranslator;
import org.candlepin.dto.api.v1.ConsumerDTO;
import org.candlepin.model.Consumer;
import org.candlepin.model.ConsumerCurator;
import org.candlepin.model.ConsumerType;
import org.candlepin.model.ConsumerType.ConsumerTypeEnum;
import org.candlepin.model.ConsumerTypeCurator;
import org.candlepin.model.EnvironmentCurator;
import org.candlepin.model.HypervisorId;
import org.candlepin.model.Owner;
import org.candlepin.model.OwnerCurator;
import org.candlepin.model.VirtConsumerMap;
import org.candlepin.resource.ConsumerResource;
import org.candlepin.service.SubscriptionServiceAdapter;
import org.candlepin.service.impl.HypervisorUpdateAction;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.xnap.commons.i18n.I18n;
import org.xnap.commons.i18n.I18nFactory;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * HypervisorUpdateJobTest
 */
public class HypervisorUpdateJobTest {

    private Owner owner;
    private Principal principal;
    private String hypervisorJson;

    private ObjectMapper objectMapper;
    private OwnerCurator ownerCurator;
    private ConsumerCurator consumerCurator;
    private ConsumerResource consumerResource;
    private ConsumerTypeCurator consumerTypeCurator;
    private HypervisorUpdateAction hypervisorUpdateAction;
    private I18n i18n;
    private SubscriptionServiceAdapter subAdapter;
    private Configuration config;

    private ModelTranslator translator;

    @BeforeEach
    public void init() {
        i18n = I18nFactory.getI18n(
            getClass(),
            Locale.US,
            I18nFactory.READ_PROPERTIES | I18nFactory.FALLBACK
        );
        owner = mock(Owner.class);
        principal = mock(Principal.class);
        ownerCurator = mock(OwnerCurator.class);
        consumerCurator = mock(ConsumerCurator.class);
        consumerResource = mock(ConsumerResource.class);
        consumerTypeCurator = mock(ConsumerTypeCurator.class);
        subAdapter = mock(SubscriptionServiceAdapter.class);
        config = new CandlepinCommonTestConfig();

        objectMapper = new ObjectMapper();
        when(owner.getId()).thenReturn("joe");

        ConsumerType ctype = new ConsumerType(ConsumerTypeEnum.HYPERVISOR);
        ctype.setId("test-ctype");

        when(consumerTypeCurator.getByLabel(eq(ConsumerTypeEnum.HYPERVISOR.getLabel()))).thenReturn(ctype);
        when(consumerTypeCurator.getByLabel(eq(ConsumerTypeEnum.HYPERVISOR.getLabel()), anyBoolean()))
            .thenReturn(ctype);

        when(owner.getKey()).thenReturn("joe");
        when(principal.getUsername()).thenReturn("joe user");

        EnvironmentCurator environmentCurator = mock(EnvironmentCurator.class);
        translator = new StandardTranslator(consumerTypeCurator, environmentCurator, ownerCurator);

        hypervisorJson =
            "{\"hypervisors\":" +
                "[{" +
                "\"name\" : \"hypervisor_999\"," +
                "\"hypervisorId\" : {\"hypervisorId\":\"uuid_999\"}," +
                "\"guestIds\" : [{\"guestId\" : \"guestId_1_999\"}]" +
                "}]}";

        hypervisorUpdateAction = new HypervisorUpdateAction(
            consumerCurator, consumerTypeCurator, consumerResource, subAdapter, translator, config);
    }

    @Test
    public void createJobDetail() {
        JobConfig config = createJobConfig(null);

        assertDoesNotThrow(config::validate);
    }

    @Test
    public void ownerMustBePresent() {
        JobConfig config = HypervisorUpdateJob.createJobConfig()
            .setData(hypervisorJson)
            .setCreateMissing(true)
            .setPrincipal(principal)
            .setReporter(null);

        assertThrows(JobConfigValidationException.class, config::validate);
    }

    @Test
    public void createFlagMustBePresent() {
        JobConfig config = HypervisorUpdateJob.createJobConfig()
            .setOwner(owner)
            .setData(hypervisorJson)
            .setPrincipal(principal)
            .setReporter(null);

        assertThrows(JobConfigValidationException.class, config::validate);
    }

    @Test
    public void hypervisorDataMustBePresent() {
        JobConfig config = HypervisorUpdateJob.createJobConfig()
            .setOwner(owner)
            .setCreateMissing(true)
            .setPrincipal(principal)
            .setReporter(null);

        assertThrows(JobConfigValidationException.class, config::validate);
    }

    @Test
    public void hypervisorUpdateExecCreate() throws JobExecutionException {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);

        JobConfig config = createJobConfig(null);

        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
        verify(consumerCurator).saveAll(anySet(), eq(false), eq(false));
    }

    @Test
    public void reporterIdOnCreateTest() throws JobExecutionException {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);

        JobConfig config = createJobConfig("createReporterId");
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
        ArgumentCaptor<Set<Consumer>> argument = createConsumersCaptor();
        verify(consumerCurator).saveAll(argument.capture(), eq(false), eq(false));
        String actualReporterId = firstHypervisorId(argument, HypervisorId::getReporterId);
        assertEquals("createReporterId", actualReporterId);
    }

    @Test
    public void hypervisorUpdateExecUpdate() throws JobExecutionException {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);
        Consumer hypervisor = new Consumer();
        hypervisor.ensureUUID();
        hypervisor.setName("hypervisor_name");
        hypervisor.setOwner(owner);
        String hypervisorId = "uuid_999";
        hypervisor.setHypervisorId(new HypervisorId(hypervisorId));
        VirtConsumerMap vcm = new VirtConsumerMap();
        vcm.add(hypervisorId, hypervisor);
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(vcm);

        JobConfig config = createJobConfig(null);
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
        verify(consumerResource).checkForFactsUpdate(any(Consumer.class), any(Consumer.class));
        verify(consumerCurator, times(2)).bulkUpdate(anySet(), eq(false));
    }

    @Test
    public void reporterIdOnUpdateTest() throws JobExecutionException {
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        Consumer hypervisor = new Consumer();
        hypervisor.ensureUUID();
        hypervisor.setName("hypervisor_name");
        hypervisor.setOwner(owner);
        String hypervisorId = "uuid_999";
        hypervisor.setHypervisorId(new HypervisorId(hypervisorId));
        VirtConsumerMap vcm = new VirtConsumerMap();
        vcm.add(hypervisorId, hypervisor);
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(vcm);

        JobConfig config = createJobConfig("updateReporterId");
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
        assertEquals("updateReporterId", hypervisor.getHypervisorId().getReporterId());
    }

    @Test
    public void hypervisorIdIsOverridenDuringHypervisorReportTest() throws JobExecutionException {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);
        Consumer hypervisor = new Consumer();
        hypervisor.ensureUUID();
        hypervisor.setName("hyper-name");
        hypervisor.setOwner(owner);
        hypervisor.setFact(Consumer.Facts.SYSTEM_UUID, "myUuid");
        String hypervisorId = "existing_hypervisor_id";
        hypervisor.setHypervisorId(new HypervisorId(hypervisorId));
        VirtConsumerMap vcm = new VirtConsumerMap();
        vcm.add(hypervisorId, hypervisor);
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(vcm);

        hypervisorJson =
            "{\"hypervisors\":" +
                "[{" +
                "\"name\" : \"hypervisor_999\"," +
                "\"hypervisorId\" : {\"hypervisorId\":\"expectedHypervisorId\"}," +
                "\"guestIds\" : [{\"guestId\" : \"guestId_1_999\"}]," +
                "\"facts\" : {\"dmi.system.uuid\" : \"myUuid\"}" +
                "}]}";

        JobConfig config = createJobConfig(null);
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);

        ArgumentCaptor<Set<Consumer>> updateCaptor = createConsumersCaptor();
        verify(consumerCurator, times(2)).bulkUpdate(updateCaptor.capture(), eq(false));

        String actualHypervisorId = firstHypervisorId(updateCaptor, HypervisorId::getHypervisorId);
        assertEquals("expectedhypervisorid", actualHypervisorId);
    }

    @Test
    public void hypervisorUpdateExecCreateNoHypervisorId() throws JobExecutionException {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);

        hypervisorJson =
            "{\"hypervisors\":" +
                "[{" +
                "\"name\" : \"hypervisor_999\"," +
                "\"guestIds\" : [{\"guestId\" : \"guestId_1_999\"}]" +
                "}]}";

        JobConfig config = createJobConfig(null);
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
        verify(consumerResource, never()).createConsumerFromDTO(any(ConsumerDTO.class),
            any(ConsumerType.class), any(Principal.class), anyString(), anyString(), anyString(),
            eq(false));
    }

    @Test
    public void hypervisorUpdateIgnoresEmptyGuestIds() throws Exception {
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);
        when(ownerCurator.findOwnerById(eq("joe"))).thenReturn(owner);

        hypervisorJson =
            "{\"hypervisors\":" +
                "[{" +
                "\"hypervisorId\" : {\"hypervisorId\" : \"hypervisor_999\"}," +
                "\"name\" : \"hypervisor_999\"," +
                "\"guestIds\" : [{\"guestId\" : \"guestId_1_999\"}, {\"guestId\" : \"\"}]" +
                "}]}";

        JobConfig config = createJobConfig(null);
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());

        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);
        job.execute(ctx);
    }

    @Test
    public void ensureJobFailsWhenAutobindDisabledForTargetOwner() {
        // Disabled autobind
        when(owner.isAutobindDisabled()).thenReturn(true);
        when(ownerCurator.getByKey(eq("joe"))).thenReturn(owner);

        JobConfig config = createJobConfig(null);
        JobExecutionContext ctx = mock(JobExecutionContext.class);
        when(ctx.getJobArguments()).thenReturn(config.getJobArguments());
        when(consumerCurator.getHostConsumersMap(eq(owner), Mockito.<Consumer>anyList()))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateJob job = new HypervisorUpdateJob(ownerCurator, consumerCurator,
            translator, hypervisorUpdateAction, i18n, objectMapper);

        JobExecutionException e = assertThrows(JobExecutionException.class, () -> job.execute(ctx));
        assertThat(e.getMessage(),
            containsString("Could not update host/guest mapping. Auto-attach is disabled for owner joe."));
    }

    private JobConfig createJobConfig(final String reporterId) {
        return HypervisorUpdateJob.createJobConfig()
            .setOwner(owner)
            .setData(hypervisorJson)
            .setCreateMissing(true)
            .setPrincipal(principal)
            .setReporter(reporterId);
    }

    private ArgumentCaptor<Set<Consumer>> createConsumersCaptor() {
        Class<Set<Consumer>> consumersClass = (Class<Set<Consumer>>) (Class) Set.class;
        return ArgumentCaptor.forClass(consumersClass);
    }

    private String firstHypervisorId(ArgumentCaptor<Set<Consumer>> captor,
        Function<HypervisorId, String> action) {
        return captor.getValue().stream()
            .map(Consumer::getHypervisorId)
            .map(action)
            .findFirst()
            .orElse(null);
    }
}
