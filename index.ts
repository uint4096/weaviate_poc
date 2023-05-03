import weaviate, { WeaviateClient } from 'weaviate-ts-client';

(async () => {
    const client: WeaviateClient = weaviate.client({
        scheme: 'http',
        host: process.env.HOST || '',
        headers: { 'X-OpenAI-API-KEY': process.env.OPENAI_APIKEY || '' },
    });

    const className = 'Question';

    const createClassIfDoesntExist = async () => {
        try {
            await client.schema.classGetter().withClassName(className).do();
        } catch (e) {
            const weaviateClass = {
                class: className,
                vectorizer: 'text2vec-openai'
            };
            await client.schema.classCreator().withClass(weaviateClass).do();
        }
    };

    const insertInBatches = async (
        input: Array<string>, 
        batchSize: number
    ) => {
        let batcher = client.batch.objectsBatcher();
        let counter = 0, inserted = 0;
        while (inserted < input.length) {
            const payload = {
                class: className,
                properties: { key: input[inserted] },
            };

            inserted += 1;
            counter += 1;
            batcher.withObject(payload);

            if (counter === batchSize || inserted === input.length) {
                try {
                    await batcher.do();
                    batcher = client.batch.objectsBatcher();
                    counter = 0;
                } catch (e) {
                    console.log(`Error while inserting a batch: ${e}`);
                }
            }
        }
    };

    const makeQuery = async (query: string, limit: number) => {
        try {
            const res = await client
                .graphql
                .get()
                .withClassName(className)
                .withFields('key')
                .withHybrid({ query })
                .withLimit(limit)
                .do();

            return res?.data?.Get?.[className]?.map((c: { key: string }) => c?.key);
        } catch (e) {
            console.log(`Error while making query: ${e}`);
        }
    };

    const getCount = async () => {
        try {
            const countResponse = await client.graphql
                .aggregate()
                .withClassName(className)
                .withFields('meta { count }')
                .do();

            return countResponse?.data?.Aggregate?.[className]?.[0]?.meta.count;
        } catch (e) {
            console.log(`Error while getting count: ${e}`);
        }
    };

    const createObject = async (key: string) => {
        try {
            await client.data
                .creator()
                .withClassName(className)
                .withProperties({ key })
                .do();
        } catch (e) {
            console.log(`Error while creating object: ${e}`);
        }
    };

    const deleteClass = async () => {
        try {
            await client.schema
                .classDeleter()
                .withClassName(className)
                .do();
        } catch (e) {
            console.log(`Error while deleting class ${className}: ${e}`);
        }
    };

    // const meetingNoteTitles = [
        // "Client Discovery Call Summary",
        // "Stakeholder Alignment and Goal Setting",
        // "Quarterly Business Review",
        // "Customer Success Plan Review",
        // "Product Demo and Q&A Session",
        // "Competitive Analysis and Market Insights",
        // "Sales Pipeline Review and Forecasting",
        // "Customer Churn Analysis and Improvement Plan",
        // "Market Research and Opportunity Analysis",
        // "Marketing Campaign Performance Review",
    // ];

    await createClassIfDoesntExist();
    // await insertInBatches(meetingNoteTitles, 3);
    // console.log(`Total count: ${await getCount()}`);
    console.log(await makeQuery('product demo', 4));

    await createObject('Product Demo and Competitive Analysis Discussion');
    console.log(await makeQuery('product demo', 4));
    // await deleteClass();
    console.log(`Total count: ${await getCount()}`);
})();
