/**
 * Copyright (c) 2009 Red Hat, Inc.
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
package org.fedoraproject.candlepin.resource;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.fedoraproject.candlepin.model.EntitlementPool;

/**
 * This object exists only to expose entitlement pools as a collection with the
 * pattern. This is a workaroung object for jeresy
 * https://jersey.dev.java.net/issues/show_bug.cgi?id=361
 * <pools><pool></pool></pools>
 */
@XmlRootElement
public class Pools {
    public List<EntitlementPool> pool;
}
