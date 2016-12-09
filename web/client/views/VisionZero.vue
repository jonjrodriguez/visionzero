<template>
    <div>
        <TitleHeader title="Vision Zero" />

        <h3>What is Vision Zero?</h3>

        <blockquote class="blockquote" style="font-size:1rem">
            <p>In the first days of our administration, we made a commitment to decisively confront the epidemic of traffic fatalities and injuries on our streets.</p>
            <p>The fundamental message of Vision Zero is that death and injury on city streets is not acceptable and that we will no longer regard serious crashes as inevitable.</p>
            <footer class="blockquote-footer">Mayor Bill de Blasio</footer>
        </blockquote>

        <h3>Has it been effective?</h3>
        <p>We will look at some of the primary initiatives:</p>
        <ol>
            <li><strong>Enhanced Law Enforcement:</strong> Specifically Speeding Violations</li>
            <li><strong>25 MPH citywide speed limit:</strong> Analyzed through TLC ride data</li>
        </ol>


        <p>More info at: <a href="http://www1.nyc.gov/site/visionzero/index.page">nyc.gov</a></p>
    </div>
</template>

<script>
    import TitleHeader from "../components/Header.vue";

    export default {
        name: "VisionZero",

        components: { TitleHeader }
    }
</script>