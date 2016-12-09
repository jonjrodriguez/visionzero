<template>
    <div class="dropdown" :class="{'nav-item': nav}">
        <a href="#" class="dropdown-toggle" :class="{'nav-link': nav}" @click="toggle">
            {{ label }}
        </a>
        <div class="dropdown-menu">
            <slot></slot>
        </div>
    </div>
</template>

<script>
   export default {
        props: {
            label: {
                type: String,
                required: true,
            },

            nav: {
                type: Boolean,
                default: false
            }
        },

        mounted() {
            document.addEventListener("click", this.closeOnWindowClick);
        },

        beforeDestroy() {
            document.removeEventListener("click", this.closeOnWindowClick);
        },

        methods: {
            toggle(e) {
                e.preventDefault();

                this.$el.classList.toggle('open');
            },

            closeOnWindowClick(e) {
                if (this.$el.children[0] !== e.target) {
                    this.$el.classList.remove('open');
                }
            }
        }
    }
</script>