package org.ict4htw.atomfeed.client.repository.datasource;

import org.ict4htw.atomfeed.spring.resource.EventResource;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;

import static org.mockito.Mockito.when;

public class WebClientStub extends WebClient {
    private EventResource eventResource;
    private HttpServletRequest httpServletRequest;

    public WebClientStub(EventResource eventResource) {
        this.eventResource = eventResource;
        httpServletRequest = Mockito.mock(HttpServletRequest.class);
    }

    @Override
    public String fetch(URI uri) {
    	when(httpServletRequest.getRequestURL()).thenReturn(new StringBuffer(uri.toString()));
        String feedId = uri.getPath().replace("/", "");
        try {
            return eventResource.getEventFeed(httpServletRequest, Integer.valueOf(feedId));
        } catch (NumberFormatException e) {
            return eventResource.getRecentEventFeed(httpServletRequest);
        }
    }
}