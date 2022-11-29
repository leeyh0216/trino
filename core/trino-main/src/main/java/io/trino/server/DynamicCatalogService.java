package io.trino.server;

import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.stream.Collectors.joining;

public class DynamicCatalogService {

    private final Announcer announcer;
    private final CatalogManager catalogManager;

    @Inject
    public DynamicCatalogService(Announcer announcer, CatalogManager catalogManager) {
        this.announcer = announcer;
        this.catalogManager = catalogManager;
    }

    public void refreshCatalogHandleIds(){
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // automatically build catalogHandleIds if not configured
        String catalogHandleIds = catalogManager.getCatalogNames().stream()
                .map(catalogManager::getCatalog)
                .flatMap(Optional::stream)
                .map(Catalog::getCatalogHandle)
                .map(CatalogHandle::getId)
                .distinct()
                .sorted()
                .collect(joining(","));

        // build announcement with updated sources
        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        Map<String, String> newProperties = new HashMap<>(announcement.getProperties());
        newProperties.remove("catalogHandleIds");
        newProperties.put("catalogHandleIds", catalogHandleIds);
        builder.addProperties(newProperties);

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
    }
}
