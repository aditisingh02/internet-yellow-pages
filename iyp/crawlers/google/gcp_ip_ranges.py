import argparse
import logging
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler

URL = 'https://www.gstatic.com/ipranges/cloud.json'
ORG = 'Google'
NAME = 'google.gcp_ip_ranges'

# GCP region to country code mapping based on Google Cloud documentation
# https://cloud.google.com/compute/docs/regions-zones
# Region names follow pattern: <continent/country>-<location><number>
REGION_TO_COUNTRY = {
    # Africa
    'africa-south1': 'ZA',  # Johannesburg, South Africa
    # Asia Pacific
    'asia-east1': 'TW',  # Changhua County, Taiwan
    'asia-east2': 'HK',  # Hong Kong
    'asia-northeast1': 'JP',  # Tokyo, Japan
    'asia-northeast2': 'JP',  # Osaka, Japan
    'asia-northeast3': 'KR',  # Seoul, South Korea
    'asia-south1': 'IN',  # Mumbai, India
    'asia-south2': 'IN',  # Delhi, India
    'asia-southeast1': 'SG',  # Jurong West, Singapore
    'asia-southeast2': 'ID',  # Jakarta, Indonesia
    # Australia
    'australia-southeast1': 'AU',  # Sydney, Australia
    'australia-southeast2': 'AU',  # Melbourne, Australia
    # Europe
    'europe-central2': 'PL',  # Warsaw, Poland
    'europe-north1': 'FI',  # Hamina, Finland
    'europe-north2': 'SE',  # Stockholm, Sweden
    'europe-southwest1': 'ES',  # Madrid, Spain
    'europe-west1': 'BE',  # St. Ghislain, Belgium
    'europe-west2': 'GB',  # London, UK
    'europe-west3': 'DE',  # Frankfurt, Germany
    'europe-west4': 'NL',  # Eemshaven, Netherlands
    'europe-west6': 'CH',  # Zurich, Switzerland
    'europe-west8': 'IT',  # Milan, Italy
    'europe-west9': 'FR',  # Paris, France
    'europe-west10': 'DE',  # Berlin, Germany
    'europe-west12': 'IT',  # Turin, Italy
    # Middle East
    'me-central1': 'QA',  # Doha, Qatar
    'me-central2': 'SA',  # Dammam, Saudi Arabia
    'me-west1': 'IL',  # Tel Aviv, Israel
    # North America
    'northamerica-northeast1': 'CA',  # Montreal, Canada
    'northamerica-northeast2': 'CA',  # Toronto, Canada
    'northamerica-south1': 'MX',  # Querétaro, Mexico
    'us-central1': 'US',  # Council Bluffs, Iowa, USA
    'us-east1': 'US',  # Moncks Corner, South Carolina, USA
    'us-east4': 'US',  # Ashburn, Virginia, USA
    'us-east5': 'US',  # Columbus, Ohio, USA
    'us-south1': 'US',  # Dallas, Texas, USA
    'us-west1': 'US',  # The Dalles, Oregon, USA
    'us-west2': 'US',  # Los Angeles, California, USA
    'us-west3': 'US',  # Salt Lake City, Utah, USA
    'us-west4': 'US',  # Las Vegas, Nevada, USA
    # South America
    'southamerica-east1': 'BR',  # São Paulo, Brazil
    'southamerica-west1': 'CL',  # Santiago, Chile
}


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = (
            'https://cloud.google.com/compute/docs/faq#find_ip_range'
        )

    def run(self):
        """Fetch GCP IP ranges and push to IYP."""

        # Fetch GCP IP ranges JSON
        logging.info(f'Fetching GCP IP ranges from {URL}')
        resp = requests.get(URL)
        resp.raise_for_status()
        data = resp.json()

        # Parse creationTime for reference_time_modification
        # Format: 'YYYY-MM-DDTHH:MM:SS.ffffff'
        if 'creationTime' in data:
            try:
                dt = datetime.fromisoformat(data['creationTime'])
                self.reference['reference_time_modification'] = dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logging.warning(f"Could not parse creationTime: {data['creationTime']}")

        # Parse prefixes
        items = []
        for item in data.get('prefixes', []):
            if 'ipv4Prefix' in item:
                items.append({
                    'prefix': item['ipv4Prefix'],
                    'service': item.get('service', 'Google Cloud'),
                    'scope': item.get('scope', ''),
                    'af': 4
                })
            if 'ipv6Prefix' in item:
                items.append({
                    'prefix': item['ipv6Prefix'],
                    'service': item.get('service', 'Google Cloud'),
                    'scope': item.get('scope', ''),
                    'af': 6
                })

        logging.info(f'Processing {len(items)} prefixes')

        # Collect unique values
        prefixes = set()
        services = set()
        countries = set()

        for item in items:
            prefixes.add(item['prefix'])
            services.add(item['service'])
            scope = item['scope']
            if scope in REGION_TO_COUNTRY:
                countries.add(REGION_TO_COUNTRY[scope])
            elif scope:
                logging.debug(f'Unknown GCP region/scope: {scope}')

        # Create/get nodes
        logging.info(f'Creating {len(prefixes)} GeoPrefix nodes')
        prefix_id = self.iyp.batch_get_nodes_by_single_prop(
            'GeoPrefix', 'prefix', prefixes, all=False
        )
        # Add Prefix label to all GeoPrefix nodes
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')

        tag_id = self.iyp.batch_get_nodes_by_single_prop(
            'Tag', 'label', services, all=False
        )

        country_id = self.iyp.batch_get_nodes_by_single_prop(
            'Country', 'country_code', countries, all=False
        )

        # Prepare relationships
        categorized_links = []
        country_links = []

        for item in items:
            prefix = item['prefix']
            if prefix not in prefix_id:
                continue

            p_id = prefix_id[prefix]

            # CATEGORIZED -> Tag (service)
            service = item['service']
            if service in tag_id:
                categorized_links.append({
                    'src_id': p_id,
                    'dst_id': tag_id[service],
                    'props': [self.reference]
                })

            # COUNTRY -> Country
            scope = item['scope']
            if scope in REGION_TO_COUNTRY:
                cc = REGION_TO_COUNTRY[scope]
                if cc in country_id:
                    country_links.append({
                        'src_id': p_id,
                        'dst_id': country_id[cc],
                        'props': [self.reference]
                    })

        # Create relationships
        logging.info(f'Creating {len(categorized_links)} CATEGORIZED relationships')
        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

        logging.info(f'Creating {len(country_links)} COUNTRY relationships')
        self.iyp.batch_add_links('COUNTRY', country_links)

        logging.info(f'Finished processing GCP IP ranges: {len(items)} prefixes')

    def unit_test(self):
        return super().unit_test(['CATEGORIZED', 'COUNTRY'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME.replace('.', '_') + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
