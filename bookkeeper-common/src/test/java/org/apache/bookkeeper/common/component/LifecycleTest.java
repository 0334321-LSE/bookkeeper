package org.apache.bookkeeper.common.component;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LifecycleTest {

    @Mock
    Lifecycle dummyLifecycle = mock(Lifecycle.class);

    @Before
    public void createMocks() {
        when(dummyLifecycle.state()).thenReturn(Lifecycle.State.INITIALIZED);
    }

    @Test
    public void stateTest(){
        Assert.assertEquals(dummyLifecycle.state(),Lifecycle.State.INITIALIZED);
    }
}