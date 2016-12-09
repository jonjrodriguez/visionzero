<template>
    <div>
        <TitleHeader title="Charts" />

        <div class="nav nav-pills">
            <DropDown :label="source | capitalize" nav v-for="source in sources">
                <a class="dropdown-item" href="#" v-for="file in files" @click.prevent="fetchData(source, file)">{{ file | title }}</a>
            </DropDown>
            <div class="nav-item">
                <div class="nav-link">
                    <label class="form-check-label">
                        <input class="form-check-input" type="checkbox" v-model="realtime" style="position:absolute">
                        Realtime
                    </label>
                </div>
            </div>
            <div class="nav-item">
                <pulse-loader class="nav-link" :loading="loading"></pulse-loader>
                <span v-if="error" class="text-danger nav-link">{{ error }}</span>
            </div>
            <p class="nav-item float-xs-right">
                <button
                    class="nav-link btn btn-primary"
                    :disabled="query == ''"
                    @click="showQuery = !showQuery">
                    View Impala Query
                </button>
            </p>
        </div>

        <hr />

        <div class="row">
            <div :class="[showQuery ? 'col-sm-8' : 'col-sm-12']">
                <v-client-table
                    style="text-align:center"
                    :data="tableData"
                    :columns="columns"
                    :options="options" />
            </div>
            <div class="col-sm-4" v-show="showQuery">
                <pre><code>{{ query }}</code></pre>
            </div>
        </div>
    </div>
</template>

<script>
    import axios from "axios";
    import PulseLoader from 'vue-spinner/src/PulseLoader.vue';
    import TitleHeader from "../components/Header.vue";
    import DropDown from "../components/DropDown.vue";

    export default {
        name: "Charts",

        components: { TitleHeader, DropDown, PulseLoader },

        data() {
            return {
                loading: false,
                realtime: false,
                query: "",
                showQuery: false,
                error: "",
                sources: ['collisions', 'violations', 'tlc'],
                files: ['by_month', "running_avg", "percent_change"],
                columns: [],
                tableData: [],
                options: {
                    filterable: false,
                    perPage: 50,
                    orderBy: {
                        column: "yeard"
                    }
                }
            }
        },

        methods: {
            fetchData(source, file) {
                this.loading = true;
                this.error = "";
                this.query = "";

                axios.get(`api/${source}_${file}`, { params: { realtime: this.realtime } })
                    .then(({ data }) => {
                        this.loading = false;
                        
                        if (typeof data === 'string') {
                            data = data.replace(/\bInfinity\b/g, "null");
                            data = data.replace(/\bNaN\b/g, "null");
                            data = JSON.parse(data);
                        }

                        this.query = data.query.replace(/           (?= )/g, "");
                        this.columns = this.getColumns(data.results);
                        this.tableData = this.getData(data.results);
                    }, error => {
                        this.loading = false;
                        this.error = "Error retrieving data";
                        this.columns = [];
                        this.tableData = [];
                        console.log(error);
                    });
            },

            getColumns(data) {
                return data[0];
            },

            getData(data) {
                const headers = data[0];
                const length = headers.length;

                return data.slice(1).map(row => {
                    let formattedRow = {};
                    for (let i = 0; i < length; i++) {
                        formattedRow[headers[i]] = row[i];
                    }

                    return formattedRow;
                });
            }
        }
    }
</script>

<style>
    .pull-right {
        float: right;
    }

    .pull-right label {
        padding-right: 10px;
    }
</style>