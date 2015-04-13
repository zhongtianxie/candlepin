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
package org.candlepin.liquibase;

import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;



/**
 * The MultiOrgUpgradeTask performs the post-db upgrade data migration to the cpo_* tables.
 */
public class MultiOrgUpgradeTask {

    private JdbcConnection connection;
    private CustomTaskLogger logger;

    private Map<String, PreparedStatement> preparedStatements;


    public MultiOrgUpgradeTask(JdbcConnection connection) {
        this(connection, new SystemOutLogger());
    }

    public MultiOrgUpgradeTask(JdbcConnection connection, CustomTaskLogger logger) {
        if (connection == null) {
            throw new IllegalArgumentException("connection is null");
        }

        if (logger == null) {
            throw new IllegalArgumentException("logger is null");
        }

        this.connection = connection;
        this.logger = logger;

        this.preparedStatements = new HashMap<String, PreparedStatement>();
    }


    protected PreparedStatement prepareStatement(String sql, Object... argv)
        throws DatabaseException, SQLException {

        PreparedStatement statement = this.preparedStatements.get(sql);
        if (statement == null) {
            statement = this.connection.prepareStatement(sql);
            this.preparedStatements.put(sql, statement);
        }

        statement.clearParameters();

        for (int i = 0; i < argv.length; ++i) {
            if (argv[i] != null) {
                statement.setObject(i + 1, argv[i]);
            }
            else {
                // Impl note:
                // Oracle has trouble with setNull. See the comments on this SO question for details:
                // http://stackoverflow.com/questions/11793483/setobject-method-of-preparedstatement
                statement.setNull(i + 1, Types.VARCHAR);
            }
        }

        return statement;
    }

    /**
     * Executes the given SQL query.
     *
     * @param sql
     *  The SQL to execute. The given SQL may be parameterized.
     *
     * @param argv
     *  The arguments to use when executing the given query.
     *
     * @return
     *  A ResultSet instance representing the result of the query.
     */
    protected ResultSet executeQuery(String sql, Object... argv) throws DatabaseException, SQLException {
        PreparedStatement statement = this.prepareStatement(sql, argv);
        return statement.executeQuery();
    }

    /**
     * Executes the given SQL update/insert.
     *
     * @param sql
     *  The SQL to execute. The given SQL may be parameterized.
     *
     * @param argv
     *  The arguments to use when executing the given update.
     *
     * @return
     *  The number of rows affected by the update.
     */
    protected int executeUpdate(String sql, Object... argv) throws DatabaseException, SQLException {
        PreparedStatement statement = this.prepareStatement(sql, argv);
        return statement.executeUpdate();
    }

    /**
     * Generates a 32-character UUID to use with object creation/migration.
     * <p/>
     * The UUID is generated by creating a "standard" UUID and removing the hyphens. The UUID may be
     * standardized by reinserting the hyphens later, if necessary.
     *
     * @return
     *  a 32-character UUID
     */
    protected String generateUUID() {
        // Maybe this method should move to Utils and be called from there?
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Executes the multi-org upgrade task.
     *
     * @throws DatabaseException
     *  if an error occurs while performing a database operation
     *
     * @throws SQLException
     *  if an error occurs while executing an SQL statement
     */
    public void execute() throws DatabaseException, SQLException {

        // Store the connection's auto commit setting, so we may temporarily clobber it.
        boolean autocommit = this.connection.getAutoCommit();
        this.connection.setAutoCommit(false);

        ResultSet orgids = this.executeQuery("SELECT id FROM cp_owner");
        while (orgids.next()) {
            String orgid = orgids.getString(1);
            this.logger.info(String.format("Migrating data for org %s", orgid));

            this.migrateProductData(orgid);
            this.migrateActivationKeyData(orgid);
            this.migratePoolData(orgid);
            this.migrateSubscriptionData(orgid);
        }

        orgids.close();

        // Commit & restore original autocommit state
        this.connection.commit();
        this.connection.setAutoCommit(autocommit);
    }

    /**
     * Migrates product data. Must be called per-org.
     *
     * @param orgid
     *  The id of the owner/organization for which to migrate product data
     */
    @SuppressWarnings("checkstyle:methodlength")
    private void migrateProductData(String orgid) throws DatabaseException, SQLException {
        Map<String, String> contentCache = new HashMap<String, String>();

        this.logger.info("Migrating product data for org " + orgid);

        ResultSet productids = this.executeQuery(
            "SELECT p.product_id_old " +
            "  FROM cp_pool p " +
            "  WHERE p.owner_id = ? " +
            "  AND NOT NULLIF(p.product_id_old, '') IS NULL " +
            "UNION " +
            "SELECT p.derived_product_id_old " +
            "  FROM cp_pool p " +
            "  WHERE p.owner_id = ? " +
            "  AND NOT NULLIF(p.derived_product_id_old, '') IS NULL " +
            "UNION " +
            "SELECT pp.product_id " +
            "  FROM cp_pool p " +
            "  JOIN cp_pool_products pp " +
            "    ON p.id = pp.pool_id " +
            "  WHERE p.owner_id = ? " +
            "  AND NOT NULLIF(pp.product_id, '') IS NULL ",
            orgid, orgid, orgid
        );

        while (productids.next()) {
            String productid = productids.getString(1);
            String productuuid = this.generateUUID();

            this.logger.info(
                String.format("Mapping org/prod %s/%s to UUID %s", orgid, productid, productuuid)
            );

            // Migration information from pre-existing tables to cpo_* tables
            this.executeUpdate(
                "INSERT INTO cpo_products " +
                "SELECT ?, created, updated, multiplier, ?, ?, name " +
                "FROM cp_product WHERE id = ?",
                productuuid, orgid, productid, productid
            );

            this.executeUpdate(
                "INSERT INTO cpo_pool_provided_products " +
                "SELECT pool_id, ? " +
                "FROM cp_pool_products WHERE product_id = ? AND dtype='provided'",
                productuuid, productid
            );

            this.executeUpdate(
                "INSERT INTO cpo_pool_derived_products " +
                "SELECT pool_id, ? " +
                "FROM cp_pool_products WHERE product_id = ? AND dtype='derived'",
                productuuid, productid
            );

            ResultSet attributes = this.executeQuery(
                "SELECT id, created, updated, name, value, product_id " +
                "FROM cp_product_attribute WHERE product_id = ?",
                productid
            );

            while (attributes.next()) {
                this.executeUpdate(
                    "INSERT INTO cpo_product_attributes" +
                    "  (id, created, updated, name, value, product_uuid) " +
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    this.generateUUID(), attributes.getDate(2), attributes.getDate(3),
                    attributes.getString(4), attributes.getString(5), productuuid
                );
            }

            attributes.close();

            ResultSet certificates = this.executeQuery(
                "SELECT id, created, updated, cert, privatekey, product_id " +
                "FROM cp_product_certificate WHERE product_id = ?",
                productid
            );

            while (certificates.next()) {
                this.executeUpdate(
                    "INSERT INTO cpo_product_certificates" +
                    "  (id, created, updated, cert, privatekey, product_uuid) " +
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    this.generateUUID(), certificates.getDate(2), certificates.getDate(3),
                    certificates.getBytes(4), certificates.getBytes(5), productuuid
                );
            }

            certificates.close();

            this.executeUpdate(
                "INSERT INTO cpo_product_dependent_products " +
                "SELECT ?, element " +
                "FROM cp_product_dependent_products WHERE cp_product_id = ?",
                productuuid, productid
            );

            // Update new product columns on existing tables:
            this.executeUpdate(
                "UPDATE cp_pool " +
                "SET product_uuid = ? " +
                "WHERE product_id_old = ? AND owner_id = ?",
                productuuid, productid, orgid
            );

            this.executeUpdate(
                "UPDATE cp_pool " +
                "SET derived_product_uuid = ? " +
                "WHERE derived_product_id_old = ? AND owner_id = ?",
                productuuid, productid, orgid
            );

            // Update product's content
            ResultSet contentids = this.executeQuery(
                "SELECT content_id FROM cp_product_content WHERE product_id = ?",
                productid
            );

            while (contentids.next()) {
                String contentid = contentids.getString(1);
                String contentuuid = contentCache.get(contentid);

                if (contentuuid == null) {
                    contentuuid = this.generateUUID();
                    contentCache.put(contentid, contentuuid);

                    // update cpo_content
                    this.executeUpdate(
                        "INSERT INTO cpo_content " +
                        "SELECT ?, id, created, updated, ?, contenturl, gpgurl, label, " +
                        "       metadataexpire, name, releasever, requiredtags, type, " +
                        "       vendor, arches " +
                        "FROM cp_content WHERE id = ?",
                        contentuuid, orgid, contentid
                    );

                    // update content tables
                    this.executeUpdate(
                        "INSERT INTO cpo_content_modified_products " +
                        "SELECT ?, element " +
                        "FROM cp_content_modified_products " +
                        "WHERE cp_content_id = ?",
                        contentuuid, contentid
                    );

                    ResultSet content = this.executeQuery(
                        "SELECT id, created, updated, contentid, enabled, environment_id " +
                        "FROM cp_env_content WHERE contentid = ?",
                        contentid
                    );

                    while (content.next()) {
                        this.executeUpdate(
                            "INSERT INTO cpo_environment_content" +
                            "  (id, created, updated, content_uuid, enabled, environment_id) " +
                            "VALUES(?, ?, ?, ?, ?, ?)",
                            this.generateUUID(), content.getDate(2), content.getDate(3),
                            contentuuid, content.getBoolean(5), content.getString(6)
                        );
                    }

                    content.close();
                }

                // update product => content links
                this.executeUpdate(
                    "INSERT INTO cpo_product_content " +
                    "SELECT ?, ?, enabled, created, updated " +
                    "FROM cp_product_content WHERE product_id = ? AND content_id = ?",
                    productuuid, contentuuid, productid, contentid
                );
            }

            contentids.close();
        }

        productids.close();
    }

    /**
     * Migrates activation key data. Must be called per-org.
     *
     * @param orgid
     *  The id of the owner/organization for which to migrate activation key data
     */
    private void migrateActivationKeyData(String orgid) throws DatabaseException, SQLException {

        this.logger.info("Migrating activation key data for org " + orgid);

        this.executeUpdate(
            "INSERT INTO cpo_activation_key_products(key_id, product_uuid) " +
            "SELECT AK.id, (SELECT uuid FROM cpo_products " +
            "  WHERE owner_id = ? AND product_id = AKP.product_id) " +
            "FROM cp_activation_key AK " +
            "  JOIN cp_activationkey_product AKP ON AKP.key_id = AK.id " +
            "WHERE AK.owner_id = ?",
            orgid, orgid
        );
    }

    /**
     * Migrates pool data. Must be called per-org.
     *
     * @param orgid
     *  The id of the owner/organization for which to migrate pool data
     */
    private void migratePoolData(String orgid) throws DatabaseException, SQLException {

        this.logger.info("Migrating pool data for org " + orgid);

        ResultSet pools = this.executeQuery("SELECT id FROM cp_pool WHERE owner_id = ?", orgid);

        while (pools.next()) {
            String poolid = pools.getString(1);

            ResultSet branding = this.executeQuery(
                "SELECT B.id, B.created, B.updated, (SELECT uuid FROM cpo_products " +
                "    WHERE owner_id = ? AND product_id = B.productid), B.type, B.name, B.productid " +
                "FROM cp_branding B " +
                "  JOIN cp_pool_branding PB ON PB.branding_id = B.id " +
                "WHERE PB.pool_id = ?",
                orgid, poolid
            );

            while (branding.next()) {
                String brandingid = branding.getString(1);
                String brandinguuid = this.generateUUID();

                this.logger.info(
                    String.format(
                        "Migrating branding details for ID (legacy: %s, migrated: %s), " +
                        "Org/product: %s, %s",
                        brandingid, brandinguuid, orgid, branding.getString(7)
                    )
                );

                this.executeUpdate(
                    "INSERT INTO cpo_branding(id, created, updated, product_uuid, type, name) " +
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    brandinguuid, branding.getDate(2), branding.getDate(3),
                    branding.getString(4), branding.getString(5), branding.getString(6)
                );

                this.executeUpdate(
                    "INSERT INTO cpo_pool_branding(pool_id, branding_id) " +
                    "VALUES(?, ?)",
                    poolid, brandinguuid
                );
            }

            branding.close();
        }

        pools.close();
    }

    /**
     * Migrates subscription data. Must be called per-org.
     *
     * @param orgid
     *  The id of the owner/organization for which to migrate subscription data
     */
    private void migrateSubscriptionData(String orgid) throws DatabaseException, SQLException {

        this.logger.info("Migrating subscription data for org " + orgid);

        ResultSet subscriptionids = this.executeQuery(
            "SELECT id FROM cp_subscription WHERE owner_id = ?",
            orgid
        );

        while (subscriptionids.next()) {
            String subid = subscriptionids.getString(1);
            String subuuid = this.generateUUID();

            this.executeUpdate(
                "INSERT INTO cpo_subscriptions " +
                "SELECT ?, created, updated, accountnumber, contractnumber, enddate, " +
                "    modified, quantity, startdate, upstream_pool_id, certificate_id, ?, " +
                "    (SELECT uuid FROM cpo_products " +
                "        WHERE owner_id = ? AND product_id = S.product_id), " +
                "    ordernumber, upstream_entitlement_id, upstream_consumer_id, " +
                "    (SELECT uuid FROM cpo_products " +
                "        WHERE owner_id = ? AND product_id = S.derivedproduct_id), " +
                "    cdn_id " +
                "FROM cp_subscription S WHERE id = ?",
                subuuid, orgid, orgid, orgid, subid
            );

            ResultSet sourcesub = this.executeQuery(
                "SELECT id, subscriptionid, subscriptionsubkey, pool_id, created, updated " +
                "FROM cp_pool_source_sub WHERE subscriptionid = ?",
                subid
            );

            while (sourcesub.next()) {
                this.executeUpdate(
                    "INSERT INTO cpo_pool_source_sub " +
                    "  (id, subscription_id, subscription_sub_key, pool_id, created, updated)" +
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    this.generateUUID(), subuuid, sourcesub.getString(3),
                    sourcesub.getString(4), sourcesub.getDate(5), sourcesub.getDate(6)
                );
            }

            sourcesub.close();

            ResultSet branding = this.executeQuery(
                "SELECT B.id, B.created, B.updated, (SELECT uuid FROM cpo_products " +
                "    WHERE owner_id = ? AND product_id = B.productid), B.type, B.name " +
                "FROM cp_branding B " +
                "JOIN cp_sub_branding SB ON SB.branding_id = B.id " +
                "WHERE SB.subscription_id = ?",
                orgid, subid
            );

            while (branding.next()) {
                String brandingid = branding.getString(1);
                String brandinguuid = this.generateUUID();

                this.executeUpdate(
                    "INSERT INTO cpo_branding(id, created, updated, product_uuid, type, name) " +
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    brandinguuid, branding.getDate(2), branding.getDate(3),
                    branding.getString(4), branding.getString(5), branding.getString(6)
                );

                this.executeUpdate(
                    "INSERT INTO cpo_subscription_branding(subscription_id, branding_id) " +
                    "VALUES(?, ?)",
                    subuuid, brandinguuid
                );
            }

            branding.close();

            this.executeUpdate(
                "INSERT INTO cpo_subscription_products " +
                "SELECT ?, (SELECT uuid FROM cpo_products " +
                "    WHERE owner_id = ? AND product_id = S.product_id) " +
                "FROM cp_subscription_products S WHERE subscription_id = ?",
                subuuid, orgid, subid
            );

            this.executeUpdate(
                "INSERT INTO cpo_sub_derived_products " +
                "SELECT ?, (SELECT uuid FROM cpo_products " +
                "    WHERE owner_id = ? AND product_id = S.product_id) " +
                "FROM cp_sub_derivedprods S WHERE subscription_id = ?",
                subuuid, orgid, subid
            );
        }

        subscriptionids.close();
    }


}
