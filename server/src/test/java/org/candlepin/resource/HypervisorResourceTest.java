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
package org.candlepin.resource;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.candlepin.audit.Event.Target;
import org.candlepin.audit.Event.Type;
import org.candlepin.audit.EventBuilder;
import org.candlepin.audit.EventFactory;
import org.candlepin.audit.EventSink;
import org.candlepin.auth.Access;
import org.candlepin.auth.SubResource;
import org.candlepin.auth.UserPrincipal;
import org.candlepin.common.config.Configuration;
import org.candlepin.common.exceptions.BadRequestException;
import org.candlepin.config.CandlepinCommonTestConfig;
import org.candlepin.dto.ModelTranslator;
import org.candlepin.dto.StandardTranslator;
import org.candlepin.dto.api.v1.GuestIdDTO;
import org.candlepin.dto.api.v1.HypervisorConsumerDTO;
import org.candlepin.dto.api.v1.HypervisorUpdateResultDTO;
import org.candlepin.model.Consumer;
import org.candlepin.model.ConsumerCurator;
import org.candlepin.model.ConsumerType;
import org.candlepin.model.ConsumerType.ConsumerTypeEnum;
import org.candlepin.model.ConsumerTypeCurator;
import org.candlepin.model.DeletedConsumerCurator;
import org.candlepin.model.EnvironmentCurator;
import org.candlepin.model.GuestId;
import org.candlepin.model.GuestIdCurator;
import org.candlepin.model.IdentityCertificate;
import org.candlepin.model.Owner;
import org.candlepin.model.OwnerCurator;
import org.candlepin.model.OwnerProductCurator;
import org.candlepin.model.VirtConsumerMap;
import org.candlepin.model.activationkeys.ActivationKeyCurator;
import org.candlepin.policy.SystemPurposeComplianceRules;
import org.candlepin.policy.js.compliance.ComplianceRules;
import org.candlepin.policy.js.compliance.ComplianceStatus;
import org.candlepin.resource.util.ConsumerBindUtil;
import org.candlepin.resource.util.ConsumerEnricher;
import org.candlepin.resource.util.GuestMigration;
import org.candlepin.service.IdentityCertServiceAdapter;
import org.candlepin.service.OwnerServiceAdapter;
import org.candlepin.service.SubscriptionServiceAdapter;
import org.candlepin.service.UserServiceAdapter;
import org.candlepin.util.FactValidator;
import org.candlepin.test.TestUtil;

import com.google.inject.util.Providers;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.xnap.commons.i18n.I18n;
import org.xnap.commons.i18n.I18nFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.inject.Provider;

@RunWith(MockitoJUnitRunner.class)
public class HypervisorResourceTest {
    @Mock private UserServiceAdapter userService;
    @Mock private IdentityCertServiceAdapter idCertService;
    @Mock private SubscriptionServiceAdapter subscriptionService;
    @Mock private OwnerServiceAdapter ownerService;
    @Mock private ConsumerCurator consumerCurator;
    @Mock private ConsumerTypeCurator consumerTypeCurator;
    @Mock private OwnerCurator ownerCurator;
    @Mock private OwnerProductCurator ownerProductCurator;
    @Mock private EventSink sink;
    @Mock private EventFactory eventFactory;
    @Mock private ActivationKeyCurator activationKeyCurator;
    @Mock private UserPrincipal principal;
    @Mock private ComplianceRules complianceRules;
    @Mock private SystemPurposeComplianceRules systemPurposeComplianceRules;
    @Mock private DeletedConsumerCurator deletedConsumerCurator;
    @Mock private ConsumerBindUtil consumerBindUtil;
    @Mock private EventBuilder consumerEventBuilder;
    @Mock private ConsumerEnricher consumerEnricher;
    @Mock private GuestIdCurator guestIdCurator;
    @Mock private EnvironmentCurator environmentCurator;
    private GuestIdResource guestIdResource;

    private ConsumerResource consumerResource;
    private I18n i18n;
    private Provider<I18n> i18nProvider = () -> i18n;
    private ConsumerType hypervisorType;
    private HypervisorResource hypervisorResource;
    private ModelTranslator modelTranslator;

    private Provider<GuestMigration> migrationProvider;
    private GuestMigration testMigration;

    @Before
    public void setupTest() {
        Configuration config = new CandlepinCommonTestConfig();

        testMigration = new GuestMigration(consumerCurator);
        migrationProvider = Providers.of(testMigration);

        this.i18n = I18nFactory.getI18n(getClass(), Locale.US, I18nFactory.FALLBACK);

        this.hypervisorType = new ConsumerType(ConsumerTypeEnum.HYPERVISOR);
        this.hypervisorType.setId("test-hypervisor-ctype");

        this.mockConsumerType(this.hypervisorType);

        this.modelTranslator = new StandardTranslator(this.consumerTypeCurator, this.environmentCurator,
            this.ownerCurator);

        this.consumerResource = new ConsumerResource(this.consumerCurator,
            this.consumerTypeCurator, null, this.subscriptionService, this.ownerService, null,
            this.idCertService, null, this.i18n, this.sink, this.eventFactory, null,
            this.userService, null, null, this.ownerCurator,
            this.activationKeyCurator, null, this.complianceRules, this.systemPurposeComplianceRules,
            this.deletedConsumerCurator, null, null, config,
            null, null, null, this.consumerBindUtil, null, null,
            new FactValidator(config, this.i18nProvider), null, consumerEnricher, migrationProvider,
            modelTranslator);

        this.guestIdResource = new GuestIdResource(this.guestIdCurator, this.consumerCurator,
            this.consumerTypeCurator, this.consumerResource, this.i18n, this.eventFactory, this.sink,
            migrationProvider, modelTranslator);

        this.hypervisorResource = new HypervisorResource(consumerResource,
            consumerCurator, consumerTypeCurator, i18n, ownerCurator, migrationProvider, modelTranslator,
            guestIdResource);

        // Ensure that we get the consumer that was passed in back from the create call.
        when(consumerCurator.create(any(Consumer.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return invocation.getArguments()[0];
            }
        });

        when(consumerCurator.create(any(Consumer.class), any(Boolean.class)))
            .thenAnswer(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return invocation.getArguments()[0];
                }
            });

        when(complianceRules.getStatus(any(Consumer.class), any(Date.class), any(Boolean.class)))
            .thenReturn(new ComplianceStatus(new Date()));

        when(ownerCurator.getByKey(any(String.class))).thenReturn(new Owner());
        when(eventFactory.getEventBuilder(any(Target.class), any(Type.class)))
            .thenReturn(consumerEventBuilder);
        when(consumerEventBuilder.setEventData(any(Consumer.class)))
            .thenReturn(consumerEventBuilder);
    }

    protected ConsumerType mockConsumerType(ConsumerType ctype) {
        if (ctype != null) {
            // Ensure the type has an ID
            if (ctype.getId() == null) {
                ctype.setId("test-ctype-" + ctype.getLabel() + "-" + TestUtil.randomInt());
            }

            when(consumerTypeCurator.getByLabel(eq(ctype.getLabel()))).thenReturn(ctype);
            when(consumerTypeCurator.getByLabel(eq(ctype.getLabel()), anyBoolean())).thenReturn(ctype);
            when(consumerTypeCurator.get(eq(ctype.getId()))).thenReturn(ctype);

            doAnswer(new Answer<ConsumerType>() {
                @Override
                public ConsumerType answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    Consumer consumer = (Consumer) args[0];
                    ConsumerTypeCurator curator = (ConsumerTypeCurator) invocation.getMock();
                    ConsumerType ctype = null;

                    if (consumer == null || consumer.getTypeId() == null) {
                        throw new IllegalArgumentException("consumer is null or lacks a type ID");
                    }

                    ctype = curator.get(consumer.getTypeId());
                    if (ctype == null) {
                        throw new IllegalStateException("No such consumer type: " + consumer.getTypeId());
                    }

                    return ctype;
                }
            }).when(consumerTypeCurator).getConsumerType(any(Consumer.class));
        }

        return ctype;
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void hypervisorCheckInCreatesNewConsumer() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("test-id");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("test-host", new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_A"),
            TestUtil.createGuestIdDTO("GUEST_B"))));

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(ownerCurator.findOwnerById(any(String.class))).thenReturn(owner);
        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE)))
            .thenReturn(true);
        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);

        Collection<HypervisorConsumerDTO> created = result.getCreated();
        assertEquals(1, created.size());

        HypervisorConsumerDTO c1 = created.iterator().next();
        assertEquals("test-host", c1.getName());
        assertEquals("admin", c1.getOwner().getKey());
    }

    private VirtConsumerMap mockHypervisorConsumerMap(String hypervisorId, Consumer c) {
        VirtConsumerMap mockMap = new VirtConsumerMap();
        mockMap.add(hypervisorId, c);
        return mockMap;
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void hypervisorCheckInUpdatesGuestIdsWhenHostConsumerExists() throws Exception {
        Owner owner = new Owner("owner-key-1", "Owner Name 1");
        owner.setId("owner-id-1");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        String hypervisorId = "test-host";
        hostGuestMap.put(hypervisorId, new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_B"))));

        Owner o = new Owner("owner-key-2", "Owner Name 2");
        o.setId("owner-id-2");

        Consumer existing = new Consumer().setUuid("test-host");
        existing.setName("test-host-name");
        existing.setOwner(o);
        existing.addGuestId(new GuestId("GUEST_A"));
        existing.setType(this.hypervisorType);

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(ownerCurator.findOwnerById(any(String.class))).thenReturn(owner);
        // Force update
        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(mockHypervisorConsumerMap(hypervisorId, existing));
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);

        List<HypervisorConsumerDTO> updated = new ArrayList<>(result.getUpdated());
        assertEquals(1, updated.size());

        HypervisorConsumerDTO c1 = updated.get(0);
        assertEquals("test-host", c1.getUuid());
        assertEquals("test-host-name", c1.getName());
        assertEquals("owner-key-1", c1.getOwner().getKey());

        assertEquals(1, existing.getGuestIds().size());
        assertEquals("GUEST_B", existing.getGuestIds().get(0).getGuestId());
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void hypervisorCheckInReportsFailuresOnCreateFailure() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("admin-id");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        String expectedHostVirtId = "test-host-id";
        hostGuestMap.put(expectedHostVirtId, new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_A"),
            TestUtil.createGuestIdDTO("GUEST_B"))));

        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        String expectedMessage = "Forced Exception.";
        RuntimeException exception = new RuntimeException(expectedMessage);

        // Simulate failure  when checking the owner
        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE))).
            thenReturn(true);
        when(consumerCurator.create(any(Consumer.class)))
            .thenThrow(exception);

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);

        List<String> failures = new ArrayList<>(result.getFailedUpdate());
        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("Problem creating unit"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void checkInCreatesNoNewConsumerWhenCreateIsFalse() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("admin-id");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("test-host", new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_A"),
            TestUtil.createGuestIdDTO("GUEST_B"))));

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);

        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE)))
            .thenReturn(true);
        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), false);

        assertEquals(null, result.getCreated());
        assertEquals(1, result.getFailedUpdate().size());

        String failed = result.getFailedUpdate().iterator().next();
        String expected = "test-host: Unable to find hypervisor in org \"admin\"";
        assertEquals(expected, failed);
    }

    @SuppressWarnings("deprecation")
    @Test(expected = BadRequestException.class)
    public void ensureBadRequestWhenNoMappingIsIncludedInRequest() {
        hypervisorResource.hypervisorUpdate(null, principal, "an-owner", false);
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void ensureEmptyHypervisorIdsAreIgnored() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("admin-id");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("", new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_A"),
            TestUtil.createGuestIdDTO("GUEST_B"))));
        hostGuestMap.put("HYPERVISOR_A", new ArrayList(Arrays.asList(TestUtil.createGuestIdDTO("GUEST_C"),
            TestUtil.createGuestIdDTO("GUEST_D"))));

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(ownerCurator.findOwnerById(any(String.class))).thenReturn(owner);

        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE)))
            .thenReturn(true);
        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);
        assertNotNull(result);
        assertEquals(1, result.getCreated().size());

        List<HypervisorConsumerDTO> created = new ArrayList<>(result.getCreated());
        assertEquals("HYPERVISOR_A", created.get(0).getName());
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void ensureEmptyGuestIdsAreIgnored() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("test-id");
        owner.setKey("test-key");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("HYPERVISOR_A", new ArrayList(
            Arrays.asList(TestUtil.createGuestIdDTO("GUEST_A"), TestUtil.createGuestIdDTO(""))));
        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(ownerCurator.findOwnerById(any(String.class))).thenReturn(owner);

        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE)))
            .thenReturn(true);
        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);
        assertNotNull(result);
        assertNotNull(result.getCreated());

        List<HypervisorConsumerDTO> created = new ArrayList<>(result.getCreated());
        assertEquals(1, created.size());
        assertEquals(1, hostGuestMap.get("HYPERVISOR_A").size());
        assertEquals("GUEST_A", hostGuestMap.get("HYPERVISOR_A").get(0).getGuestId());
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    public void treatNullGuestListsAsEmptyGuestLists() throws Exception {
        Owner owner = new Owner("admin");
        owner.setId("test-id");
        owner.setKey("test-key");

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("HYPERVISOR_A", null);
        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);
        when(ownerCurator.findOwnerById(any(String.class))).thenReturn(owner);

        when(consumerCurator.getHostConsumersMap(any(Owner.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());
        when(consumerCurator.getGuestConsumersMap(any(String.class), any(Set.class)))
            .thenReturn(new VirtConsumerMap());

        when(principal.canAccess(eq(owner), eq(SubResource.CONSUMERS), eq(Access.CREATE)))
            .thenReturn(true);
        when(idCertService.generateIdentityCert(any(Consumer.class)))
            .thenReturn(new IdentityCertificate());

        HypervisorUpdateResultDTO result = hypervisorResource.hypervisorUpdate(
            hostGuestMap, principal, owner.getKey(), true);
        assertNotNull(result);
        assertNotNull(result.getCreated());
        List<HypervisorConsumerDTO> created = new ArrayList<>(result.getCreated());
        assertEquals(1, created.size());
        assertEquals(0, hostGuestMap.get("HYPERVISOR_A").size());
    }

    @Test
    public void ensureFailureWhenAutobindIsDisabledOnOwner() {
        Owner owner = new Owner("test_admin");
        owner.setId("admin-id");
        owner.setAutobindDisabled(true);

        Map<String, List<GuestIdDTO>> hostGuestMap = new HashMap<>();
        hostGuestMap.put("HYPERVISOR_A", new ArrayList());
        when(ownerCurator.getByKey(eq(owner.getKey()))).thenReturn(owner);

        try {
            hypervisorResource.hypervisorUpdate(hostGuestMap, principal, owner.getKey(), true);
            fail("Exception should have been thrown since autobind was disabled for the owner.");
        }
        catch (BadRequestException bre) {
            assertEquals("Could not update host/guest mapping. Auto-attach is disabled for owner test_admin.",
                bre.getMessage());
        }
    }
}
