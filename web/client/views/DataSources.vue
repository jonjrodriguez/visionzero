<template>
    <div>
        <TitleHeader title="Data Sources" />

        <div class="card-deck-wrapper">
            <div class="card-deck">
                <div class="card" v-for="(source, name) in sources">
                    <h3 class="card-header">
                        <a :href="source.url">
                            {{ name | capitalize }}
                        </a>
                    </h3>

                    <div class="card-block">
                        <p class="card-text">{{ source.description }}</p>
                    </div>

                    <div class="card-block">
                        <p class="card-text">Our Usage:</p>
                        <p v-for="(desc, usage) in source.usages" class="mb-0">
                            {{ usage | title }}: {{ desc }}
                        </p>
                    </div>

                    <div class="card-block">
                        <p class="card-text">Download Aggregates:</p>
                        <p v-for="file in files" class="mb-0">
                            <a :href="`download/${name}_${file}`"
                                class="card-link"
                                download>
                                {{ file | title }}
                            </a>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
    import TitleHeader from "../components/Header.vue";

    export default {
        name: "DataSources",

        components: { TitleHeader },

        data() {
            return {
                sources: {
                    collisions: {
                        description: "The NYPD makes available traffic data with motor vehicle collisions archived by month dating back to 2011. This data includes each NYC collision along with the precinct where it occurred, the intersection, the number and types of vehicles, the number drivers/pedestrians involved, and the number of people injured or killed. Using this data, we can determine the change in motor vehicle accidents and traffic fatalities.",
                        url: "http://www.nyc.gov/html/nypd/html/traffic_reports/motor_vehicle_collision_data.shtml",
                        usages: {
                            time_frame: "01/2012 - 10/2016",
                            total_files: "290",
                            data_size: "161 MB"
                        }
                    },
                    violations: {
                        description: "The NYPD makes available traffic reports with moving violation data archived by month dating back to 2011. This data includes the number of different moving violations per precinct. Specifically, we will be looking at the number of speeding violations along with any other violations Vision Zero has prioritized. We can use this data to see if there has been an increase of enforcement on these moving violations and if so, which ones and at which precincts.",
                        url: "http://www.nyc.gov/html/nypd/html/traffic_reports/traffic_summons_reports.shtml",
                        usages: {
                            time_frame: "01/2012 - 10/2016",
                            total_files: "4372",
                            data_size: "33 MB"
                        }
                    },
                    tlc: {
                        description: "The TLC makes available the yellow and green taxi trip records archived by month dating back to 2009. These records include fields such as pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, and itemized fares. Using this data, we can calculate the average speed of each trip to see if speeds have decreased. In addition, we can compare historical fares and see if there is a noticeable increase in price.",
                        url: "http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml",
                        usages: {
                            time_frame: "01/2012 - 06/2016",
                            total_files: "89",
                            data_size: "121 GB"
                        }
                    }
                },
                files: ['by_month', "running_avg", "percent_change"]
            }
        }
    }
</script>