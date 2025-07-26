<script>
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';

	export let data;
</script>

<EvidenceDefaultLayout
	{data}
	hideSidebar={true}
	hideHeader={true}
	neverShowQueries={true}
>
	<slot slot="content" />
</EvidenceDefaultLayout>