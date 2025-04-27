import pandas as pd

# File paths
business_file_path = r'C:\Users\ngthu\Downloads\Yelp-JSON\Yelp JSON\yelp_dataset\yelp_academic_dataset_business.json'
review_file_path = r'C:\Users\ngthu\Downloads\Yelp-JSON\Yelp JSON\yelp_dataset\yelp_academic_dataset_review.json'

# Target states (using abbreviations as they appear in the dataset)
states_of_interest = ['NV',  # Las Vegas (Nevada)
                      'ON',  # Toronto (Ontario, Canada)
                      'AZ',  # Phoenix (Arizona)
                      'NC',  # Charlotte (North Carolina)
                      'PA']  # Pittsburgh (Pennsylvania)

# Target hotel names (you can add more names as needed)
hotel_names_of_interest = [
    'Marriott', 'Best Western', 'Hilton', 'Holiday Inn', 'Hyatt', 'Courtyard',
    'Radisson', 'InterContinental', 'Sheraton', 'Fairfield Inn', 'Wyndham',
    'Comfort Inn', 'La Quinta', 'Quality Inn', 'Econo Lodge', 'Days Inn',
    'DoubleTree', 'Embassy Suites', 'Hampton Inn', 'Extended Stay America'
]

# Load business data
business_df = pd.read_json(business_file_path, lines=True)

# Strip any extra spaces (but no case changes)
business_df['state'] = business_df['state'].str.strip()  # Keep the original case for state abbreviations
business_df['name'] = business_df['name'].str.strip()  # Keep the original case for hotel names

# Debug: Print first few rows of business data to check state and name columns
print("Sample Business Data (State and Name):")
print(business_df[['state', 'name']].head())

# Filter business data by state abbreviations and hotel names
filtered_business_df = business_df[business_df['state'].isin(states_of_interest) &
                                   business_df['name'].str.contains('|'.join(hotel_names_of_interest), case=False,
                                                                    na=False)]

# Debug: Check the filtered business data by state and hotel name
print("\nFiltered Business Data by State and Hotel Names:")
print(filtered_business_df[['state', 'name']].head())

if not filtered_business_df.empty:
    print("\nFiltered Business Data Preview:")
    print(filtered_business_df.head())
    business_output_path = r'C:\Users\ngthu\Documents\filtered_business_data_by_state_and_hotel4.csv'
    filtered_business_df.to_csv(business_output_path, index=False)
    print(f"Business data saved to: {business_output_path}")
else:
    print("No business data found for the specified states and hotel names.")

# Get relevant business_ids
relevant_ids = set(filtered_business_df['business_id'])

# Load review data in chunks
chunk_size = 100000
review_chunks = []

# List of expanded bias/discrimination-related keywords
bias_keywords = [
    'racism', 'sexist', 'homophobic', 'racist', 'discriminate', 'discriminated', 'homophobia', 'sexism',
    'no wheelchair access', 'no wheel chair access', 'slur', 'unprofessional', 'rude', 'ignored', 'disrespectful',
     'unequal service', 'discriminated against', 'not treated fairly', 'unwelcoming',
    'prejudiced', 'biased', 'offensive', 'insensitive', 'mistreated', 'poor treatment',  'harassment',
   'denied service', 'stereotyped', 'underappreciated', 'lack of respect', 'overlooked', 'unjustified judgment', 'hurtful', 'unfair pricing', 'stereotyping', 'ignored my concerns',
    'neglected', 'bad attitude', 'unequal treatment', 'unaccommodating', 'no regard for my needs'

]

for chunk in pd.read_json(review_file_path, lines=True, chunksize=chunk_size):
    # Filter reviews that mention the relevant businesses
    filtered_chunk = chunk[chunk['business_id'].isin(relevant_ids)]

    # Filter reviews based on bias/discrimination-related keywords in the text
    filtered_chunk = filtered_chunk[filtered_chunk['text'].str.contains('|'.join(bias_keywords), case=False, na=False)]

    # Check if any reviews passed the filtering
    if not filtered_chunk.empty:
        review_chunks.append(filtered_chunk)

    # Debugging: Print out how many reviews were matched after the filter
    print(f"Reviews after filtering for bias/discrimination in chunk: {len(filtered_chunk)}")

if review_chunks:
    # Concatenate filtered review chunks into a single DataFrame
    review_df = pd.concat(review_chunks, ignore_index=True)
    print("\nFiltered Review Data Preview:")
    print(review_df.head())

    review_output_path = r'C:\Users\ngthu\Documents\filtered_review_data_by_state_and_hotel4.csv'
    review_df.to_csv(review_output_path, index=False)
    print(f"Review data saved to: {review_output_path}")
else:
    print("No reviews found for the filtered businesses related to bias/discrimination.")