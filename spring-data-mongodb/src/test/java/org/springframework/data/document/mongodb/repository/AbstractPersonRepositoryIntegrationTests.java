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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for tests for {@link PersonRepository}.
 *
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractPersonRepositoryIntegrationTests {

    @Autowired
    protected PersonRepository repository;
    Person dave, carter, boyd,stefan,leroi;

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