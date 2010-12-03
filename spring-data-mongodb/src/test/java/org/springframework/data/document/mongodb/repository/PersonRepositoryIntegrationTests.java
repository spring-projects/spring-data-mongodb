/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class PersonRepositoryIntegrationTests {

    @Autowired
    PersonRepository repository;

    Person dave, carter, boyd, stefan, leroi;


    @Before
    public void setUp() {

        repository.deleteAll();

        dave = new Person("Dave", "Matthews");
        carter = new Person("Carter", "Beauford");
        boyd = new Person("Boyd", "Tinsley");
        stefan = new Person("Stefan", "Lessard");
        leroi = new Person("Leroi", "Moore");

        repository.save(Arrays.asList(dave, carter, boyd, stefan, leroi));
    }


    @Test
    public void findsPersonsByLastname() throws Exception {

        List<Person> result = repository.findByLastname("Beauford");
        assertThat(result.size(), is(1));
        assertThat(result, hasItem(carter));
    }


    @Test
    public void findsPersonsByFirstnameLike() throws Exception {

        List<Person> result = repository.findByFirstnameLike("Bo*");
        assertThat(result.size(), is(1));
        assertThat(result, hasItem(boyd));
    }


    @Test
    public void findsPagedPersons() throws Exception {

        Page<Person> result =
                repository.findAll(new PageRequest(1, 2, Direction.ASC,
                        "lastname"));
        assertThat(result.isFirstPage(), is(false));
        assertThat(result.isLastPage(), is(false));
        assertThat(result, hasItems(dave, leroi));
    }


    @Test
    public void executesPagedFinderCorrectly() throws Exception {

        Page<Person> page =
                repository.findByLastnameLike("*a*", new PageRequest(0, 2,
                        Direction.ASC, "lastname"));
        assertThat(page.isFirstPage(), is(true));
        assertThat(page.isLastPage(), is(false));
        assertThat(page.getNumberOfElements(), is(2));
        assertThat(page, hasItems(carter, stefan));
    }
}
