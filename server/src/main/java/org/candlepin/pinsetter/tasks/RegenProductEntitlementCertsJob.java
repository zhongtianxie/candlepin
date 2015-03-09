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
package org.candlepin.pinsetter.tasks;

import org.candlepin.controller.PoolManager;
import org.candlepin.service.SubscriptionServiceAdapter;

import com.google.inject.Inject;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * The Class RegenEntitlementCertsJob.
 */
public class RegenProductEntitlementCertsJob extends KingpinJob {

    public static final String PROD_ID = "product_id";
    public static final String LAZY_REGEN = "lazy_regen";

    private PoolManager poolManager;
    private SubscriptionServiceAdapter subAdapter;

    @Inject
    public RegenProductEntitlementCertsJob(PoolManager poolManager, SubscriptionServiceAdapter subAdapter) {
        this.poolManager = poolManager;
        this.subAdapter = subAdapter;
    }

    @Override
    public void toExecute(JobExecutionContext arg0) throws JobExecutionException {
        // TODO: This needs to be revisisted! Either it needs to step through every owner, or it
        //       needs to use the product's UUID.
        String productId = arg0.getJobDetail().getJobDataMap().getString(PROD_ID);
        boolean lazy = arg0.getJobDetail().getJobDataMap().getBoolean(LAZY_REGEN);

        // this.poolManager.regenerateCertificatesOf(subAdapter, productId, lazy);
    }
}
