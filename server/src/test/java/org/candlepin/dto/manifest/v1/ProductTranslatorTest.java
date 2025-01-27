/**
 * Copyright (c) 2009 - 2018 Red Hat, Inc.
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
package org.candlepin.dto.manifest.v1;

import static org.junit.Assert.*;

import org.candlepin.dto.AbstractTranslatorTest;
import org.candlepin.dto.ModelTranslator;
import org.candlepin.model.Content;
import org.candlepin.model.Product;
import org.candlepin.model.ProductContent;
import org.candlepin.test.TestUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Test suite for the ProductTranslator (manifest import/export) class
 */
public class ProductTranslatorTest extends
    AbstractTranslatorTest<Product, ProductDTO, ProductTranslator> {

    protected ContentTranslator contentTranslator = new ContentTranslator();
    protected ProductTranslator productTranslator = new ProductTranslator();

    protected ContentTranslatorTest contentTranslatorTest = new ContentTranslatorTest();

    @Override
    protected void initModelTranslator(ModelTranslator modelTranslator) {
        modelTranslator.registerTranslator(this.contentTranslator, Content.class, ContentDTO.class);
        modelTranslator.registerTranslator(this.productTranslator, Product.class, ProductDTO.class);
    }

    @Override
    protected ProductTranslator initObjectTranslator() {
        return this.productTranslator;
    }

    @Override
    protected Product initSourceObject() {
        Product source = new Product();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("attrib_1", "attrib_value_1");
        attributes.put("attrib_2", "attrib_value_2");
        attributes.put("attrib_3", "attrib_value_3");

        Collection<String> depProdIds = new LinkedList<>();
        depProdIds.add("dep_prod_1");
        depProdIds.add("dep_prod_2");
        depProdIds.add("dep_prod_3");

        source.setId("test_id");
        source.setName("test_name");
        source.setAttributes(attributes);
        source.setDependentProductIds(depProdIds);
        source.setUuid("test_uuid");
        source.setMultiplier(3L);

        for (int i = 0; i < 3; ++i) {
            Content content = TestUtil.createContent("content-" + i);
            source.addContent(content, true);
        }

        IntStream.rangeClosed(1, 3)
            .forEach(i -> source.addBranding(TestUtil.createProductBranding(source)));

        return source;
    }

    @Override
    protected ProductDTO initDestinationObject() {
        // Nothing fancy to do here.
        return new ProductDTO();
    }

    @Override
    protected void verifyOutput(Product source, ProductDTO dto, boolean childrenGenerated) {
        if (source != null) {
            assertEquals(source.getUuid(), dto.getUuid());
            assertEquals(source.getMultiplier(), dto.getMultiplier());
            assertEquals(source.getId(), dto.getId());
            assertEquals(source.getName(), dto.getName());
            assertEquals(source.getAttributes(), dto.getAttributes());
            assertEquals(source.getDependentProductIds(), dto.getDependentProductIds());

            assertNotNull(dto.getProductContent());

            if (childrenGenerated) {
                for (ProductContent pc : source.getProductContent()) {
                    for (ProductDTO.ProductContentDTO pcdto : dto.getProductContent()) {
                        Content content = pc.getContent();
                        ContentDTO cdto = pcdto.getContent();

                        assertNotNull(cdto);
                        assertNotNull(cdto.getId());

                        if (cdto.getId().equals(content.getId())) {
                            assertEquals(pc.isEnabled(), pcdto.isEnabled());

                            // Pass the content off to the ContentTranslatorTest to verify it
                            this.contentTranslatorTest.verifyOutput(content, cdto, true);
                        }
                    }
                }
            }
            else {
                assertTrue(dto.getProductContent().isEmpty());
            }
        }
        else {
            assertNull(dto);
        }
    }
}
